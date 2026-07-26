"""Incrementality tests: a steady-state sync only touches recent buckets."""

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
import pytest_asyncio

from edata.core.utils import get_month
from edata.database.controller import EdataDB
from edata.models import Energy, Supply
from edata.services.data_service import DataService

CUPS = "ESXXXXXXXXXXXXXXXXTEST"


def _reset_singleton() -> None:
    EdataDB._instance = None
    EdataDB._engine = None
    EdataDB._db_url = None


@pytest_asyncio.fixture
async def data_service(tmp_path) -> AsyncIterator[DataService]:
    """A DataService on an isolated on-disk database, singleton reset around it."""
    _reset_singleton()
    with (
        patch("edata.services.data_service.DatadisConnector"),
        patch("edata.services.data_service.REDataConnector"),
    ):
        service = DataService(CUPS, "user", "pwd", storage_path=str(tmp_path))
    yield service
    if EdataDB._engine is not None:
        await EdataDB._engine.dispose()
    _reset_singleton()


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


async def _seed(service: DataService, start: datetime, end: datetime) -> None:
    await service.db.add_supply(
        Supply(
            cups=CUPS,
            date_start=start,
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
