import json
import os
from tempfile import gettempdir
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from syrupy.assertion import SnapshotAssertion

from edata.models.bill import BillingRules
from edata.models.data import Energy, Power
from edata.models.supply import Contract, Supply
from edata.services.bill_service import BillService
from edata.services.data_service import DataService

ASSETS = os.path.join(os.path.dirname(__file__), "assets")


def load_json(filename):
    with open(os.path.join(ASSETS, filename), encoding="utf-8") as f:
        return json.load(f)


def load_models(filename, model):
    return [model(**d) for d in load_json(filename)]


@pytest.fixture(scope="module")
def storage_dir():
    return gettempdir()


@pytest.fixture(scope="module")
def supplies():
    return load_models("supplies.json", Supply)


@pytest.fixture(scope="module")
def contracts():
    return load_models("contracts.json", Contract)


@pytest.fixture(scope="module")
def energy():
    return load_models("energy.json", Energy)


@pytest.fixture(scope="module")
def power():
    return load_models("power.json", Power)


@pytest.fixture(scope="module")
def mock_connector(supplies, contracts, energy, power):
    mock = AsyncMock()
    mock.async_login.return_value = True
    mock.async_get_supplies.return_value = supplies
    mock.async_get_contract_detail.return_value = contracts
    mock.async_get_consumption_data.return_value = sorted(
        energy, key=lambda x: x.datetime
    )
    mock.async_get_max_power.return_value = power
    return mock


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def populated_data_service(mock_connector, energy, power, storage_dir):
    with patch("edata.services.data_service.DatadisConnector") as mock_connector_class:
        mock_connector_class.side_effect = lambda *a, **k: mock_connector

        ds = DataService(
            "ESXXXXXXXXXXXXXXXXTEST", "user", "pwd", storage_path=storage_dir
        )
        await ds.login()
        await ds.update_supplies()
        await ds.update_contracts()

        all_energy = sorted(energy, key=lambda x: x.datetime)
        start_date = all_energy[0].datetime
        end_date = all_energy[-1].datetime
        await ds.update_energy(start_date, end_date)
        await ds.update_power(start_date, end_date)

        await ds._update_daily_statistics(start_date, end_date)
        await ds._update_monthly_statistics(start_date, end_date)

        return ds


@pytest.mark.asyncio
async def test_data_fetch(
    populated_data_service, snapshot: SnapshotAssertion, energy, power
):
    ds = populated_data_service

    assert len(await ds.get_energy()) == len(energy)
    assert len(await ds.get_power()) == len(power)
    result_all = {
        "supplies": ds._supplies,
        "contracts": ds._contracts,
        "energy": await ds.get_energy(),
        "power": await ds.get_power(),
    }
    assert result_all == snapshot


@pytest.mark.asyncio
async def test_get_daily_energy(populated_data_service, snapshot: SnapshotAssertion):
    ds = populated_data_service
    daily = await ds.get_statistics("day")
    assert daily == snapshot


@pytest.mark.asyncio
async def test_get_monthly_energy(populated_data_service, snapshot: SnapshotAssertion):
    ds = populated_data_service
    monthly = await ds.get_statistics("month")
    assert monthly == snapshot


@pytest.mark.asyncio
async def test_bill_service(
    populated_data_service, snapshot: SnapshotAssertion, energy, storage_dir
):
    # Ensure data service is populated
    ds = populated_data_service

    # Initialize BillService with same configuration
    bs = BillService("ESXXXXXXXXXXXXXXXXTEST", storage_path=storage_dir)

    all_energy = sorted(energy, key=lambda x: x.datetime)
    start_date = all_energy[0].datetime
    end_date = all_energy[-1].datetime

    rules = BillingRules(
        p1_kwh_eur=0.20,
        p2_kwh_eur=0.20,
        p3_kwh_eur=0.20,
        p1_kw_year_eur=20,
        p2_kw_year_eur=10,
    )
    # Run update
    await bs.update(start_date, end_date, rules, is_pvpc=False)

    # Verify bills were created
    bills = await bs.get_bills(start_date, end_date)
    assert len(bills) > 0

    # Verify statistics were updated
    daily_stats = await bs.get_bills(start_date, end_date, "day")
    assert len(daily_stats) > 0

    monthly_stats = await bs.get_bills(start_date, end_date, "month")
    assert len(monthly_stats) > 0

    assert monthly_stats == snapshot
