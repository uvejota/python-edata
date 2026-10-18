"""Incrementality tests: a steady-state sync only touches recent buckets."""

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
import pytest_asyncio

from edata.core.utils import get_day, get_month
from edata.database.controller import EdataDB
from edata.models import Contract, Energy, Supply
from edata.models.bill import BillingRules
from edata.providers.datadis import earliest_query_start
from edata.services.bill_service import BillService
from edata.services.data_service import DataService

CUPS = "ESXXXXXXXXXXXXXXXXTEST"


@pytest_asyncio.fixture
async def data_service(tmp_path) -> AsyncIterator[DataService]:
    """A DataService on an isolated on-disk database, singleton reset around it."""
    EdataDB.reset()
    with (
        patch("edata.services.data_service.DatadisConnector"),
        patch("edata.services.data_service.REDataConnector"),
    ):
        service = DataService(CUPS, "user", "pwd", storage_path=str(tmp_path))
    yield service
    EdataDB.reset()


def _hourly_energy(start: datetime, end: datetime) -> list[Energy]:
    """Build deterministic hourly energy records over [start, end)."""
    records = []
    cursor = start
    while cursor < end:
        records.append(
            Energy(
                datetime=cursor,
                delta_h=1.0,
                consumption_kwh=0.1,
                surplus_kwh=0.0,
                generation_kwh=0.0,
                selfconsumption_kwh=0.0,
                real=True,
            )
        )
        cursor += timedelta(hours=1)
    return records


async def _seed(
    service: DataService,
    start: datetime,
    end: datetime,
    supply_start: datetime | None = None,
) -> None:
    await service.db.add_supply(
        Supply(
            cups=CUPS,
            date_start=supply_start or start,
            date_end=end,
            address=None,
            postal_code=None,
            province=None,
            municipality=None,
            distributor=None,
            point_type=5,
            distributor_code="2",
        )
    )
    await service.db.add_energy_list(CUPS, _hourly_energy(start, end))


@pytest.mark.asyncio
async def test_incremental_sync_only_loads_recent_month(
    data_service: DataService,
) -> None:
    """After a full build, an incremental sync must not reload complete months."""
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=70)
    await _seed(data_service, start, now)

    # Full build establishes completeness for every past month.
    await data_service.update_statistics(start, now)
    daily_before = await data_service.get_statistics("day")

    # Spy on the energy loads issued by a subsequent incremental sync.
    loaded_ranges: list[tuple[datetime, datetime]] = []
    original = data_service.get_energy

    async def _spy(start=None, end=None):
        loaded_ranges.append((start, end))
        return await original(start=start, end=end)

    with patch.object(data_service, "get_energy", side_effect=_spy):
        await data_service.update_statistics_incremental()

    # Something was loaded, but never anything before the current month.
    current_month = get_month(now)
    assert loaded_ranges
    assert all(rng[0] >= current_month for rng in loaded_ranges)

    # Stats are unchanged by the idempotent re-run.
    daily_after = await data_service.get_statistics("day")
    assert len(daily_after) == len(daily_before)


def _contract(start: datetime, end: datetime) -> Contract:
    return Contract(
        date_start=start,
        date_end=end,
        marketer="MARKETER",
        distributor_code="2",
        power=[4.6, 4.6],
    )


@pytest.mark.asyncio
async def test_incremental_sync_compiles_energy_older_than_the_stats(
    data_service: DataService,
) -> None:
    """Energy from before the first daily stat gets compiled (issue #361).

    Statistics used to be compiled from the supply's start only, which Datadis
    moves to the latest contract, so older energy (e.g. imported from 1.x) was
    left out of them.
    """
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = get_day(now - timedelta(days=100))
    supply_start = get_day(now - timedelta(days=40))
    await _seed(data_service, start, now, supply_start=supply_start)
    await data_service.update_statistics(supply_start, now)
    assert (await data_service.get_statistics("day"))[0].datetime == supply_start

    await data_service.update_statistics_incremental()

    daily = await data_service.get_statistics("day")
    assert daily[0].datetime == start
    assert len(daily) == len({get_day(x.datetime) for x in _hourly_energy(start, now)})
    monthly = await data_service.get_statistics("month")
    assert monthly[0].datetime == get_month(start)


@pytest.mark.asyncio
async def test_first_build_compiles_from_the_first_energy(
    data_service: DataService,
) -> None:
    """With no stats yet, they are compiled from the oldest energy record."""
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = get_day(now - timedelta(days=50))
    await _seed(
        data_service, start, now, supply_start=get_day(now - timedelta(days=10))
    )

    await data_service.update_statistics_incremental()

    assert (await data_service.get_statistics("day"))[0].datetime == start


@pytest.mark.asyncio
async def test_bills_build_from_the_first_energy(data_service: DataService) -> None:
    """With no bills yet, they are built from the oldest energy record."""
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = get_day(now - timedelta(days=50))
    supply_start = get_day(now - timedelta(days=10))
    await _seed(data_service, start, now, supply_start=supply_start)
    # the contract of the previous marketer and the current one
    await data_service.db.add_contract(CUPS, _contract(start, supply_start))
    await data_service.db.add_contract(CUPS, _contract(supply_start, now))
    bill_service = BillService(CUPS, storage_path=data_service._storage_path)

    rules = BillingRules(
        p1_kwh_eur=0.20,
        p2_kwh_eur=0.20,
        p3_kwh_eur=0.20,
        p1_kw_year_eur=20,
        p2_kw_year_eur=10,
    )
    await bill_service.update(billing_rules=rules, is_pvpc=False)

    assert await bill_service.get_first_bill_dt() == start
    assert (await bill_service.get_bills(type_="day"))[0].datetime == start


@pytest.mark.asyncio
async def test_history_start_takes_the_oldest_contract(
    data_service: DataService,
) -> None:
    """Datadis's supply start is the latest contract's: older ones count too."""
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    supply_start = get_day(now - timedelta(days=30))
    contract_start = get_day(now - timedelta(days=200))
    await _seed(data_service, supply_start, now)
    assert await data_service.get_history_start() == supply_start

    await data_service.db.add_contract(CUPS, _contract(contract_start, supply_start))
    assert await data_service.get_history_start() == contract_start

    # within the two years Datadis serves (a 1970 placeholder contract)
    await data_service.db.add_contract(
        CUPS, _contract(datetime(1970, 1, 1), supply_start)
    )
    assert await data_service.get_history_start() == earliest_query_start()


@pytest.mark.asyncio
async def test_history_start_unknown_supply(data_service: DataService) -> None:
    assert await data_service.get_history_start() is None
