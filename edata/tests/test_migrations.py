"""Tests for the 1.3.3 -> 2.0 legacy JSON migration."""

from collections.abc import AsyncIterator
from datetime import datetime
import json
import os
import shutil
from unittest.mock import patch

import pytest
import pytest_asyncio

from edata.database.controller import EdataDB
from edata.database.migrations import legacy_json_1_3_3 as legacy
from edata.services.data_service import DataService

ASSETS = os.path.join(os.path.dirname(__file__), "assets")
CUPS = "ESXXXXXXXXXXXXXXXXTEST"


def _reset_singleton() -> None:
    EdataDB._instance = None
    EdataDB._engine = None
    EdataDB._db_url = None


def _raw() -> dict:
    with open(os.path.join(ASSETS, "legacy_1.3.3.json"), encoding="utf-8") as f:
        return json.load(f)


def _install_legacy_file(storage_dir: str, cups: str = CUPS) -> str:
    """Copy the asset into the 1.3.3 storage location for ``storage_dir``."""
    dest = legacy._legacy_path(storage_dir, cups)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy(os.path.join(ASSETS, "legacy_1.3.3.json"), dest)
    return dest


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


# --- pure mappers ---


def test_to_supply_renames_fields() -> None:
    """Supply maps camelCase fields to the 2.0 snake_case model."""
    supply = legacy._to_supply(_raw()["supplies"][0])
    assert supply.cups == CUPS
    assert supply.point_type == 5
    assert supply.distributor_code == "2"
    assert supply.date_start == datetime(2023, 1, 1)


def test_to_contract_builds_power_list_dropping_none() -> None:
    """Contract power list is [p1, p2], dropping a None p2."""
    contracts = [legacy._to_contract(c) for c in _raw()["contracts"]]
    assert contracts[0].power == [4.6, 4.6]
    assert contracts[0].distributor_code == "2"
    # second contract has power_p2=None -> single-element power list
    assert contracts[1].power == [3.3]


def test_to_energy_renames_and_defaults() -> None:
    """Energy renames value_kWh and defaults generation/selfconsumption."""
    energy = legacy._to_energy(_raw()["consumptions"][2])
    assert energy.consumption_kwh == 0.3
    assert energy.surplus_kwh == 0.1
    assert energy.generation_kwh == 0
    assert energy.selfconsumption_kwh == 0
    assert energy.real is True


def test_to_power_and_pvpc_rename() -> None:
    """Power and pvpc rename their value fields to snake_case."""
    power = legacy._to_power(_raw()["maximeter"][0])
    assert power.value_kw == 3.2
    pvpc = legacy._to_pvpc(_raw()["pvpc"][0])
    assert pvpc.value_eur_kwh == 0.12
    assert pvpc.delta_h == 1


# --- migration apply ---


@pytest.mark.asyncio
async def test_apply_imports_raw_records_only(
    data_service: DataService, tmp_path
) -> None:
    """Import the raw records and ignores old *_sum aggregates."""
    _install_legacy_file(str(tmp_path))
    result = await legacy.apply(data_service.db, str(tmp_path), CUPS)

    assert result is not None
    assert result.name == "legacy_json_1_3_3"
    assert (
        result.supplies,
        result.contracts,
        result.energy,
        result.power,
        result.pvpc,
    ) == (1, 2, 5, 2, 2)

    supply = await data_service.get_supply()
    assert supply is not None and supply.cups == CUPS
    assert len(await data_service.get_contracts()) == 2
    assert len(await data_service.get_energy()) == 5
    assert len(await data_service.get_power()) == 2
    assert len(await data_service.get_pvpc()) == 2

    # The old *_sum aggregates in the file must be ignored by the migration.
    assert await data_service.get_statistics("day") == []
    assert await data_service.get_statistics("month") == []


@pytest.mark.asyncio
async def test_apply_missing_file_is_noop(data_service: DataService, tmp_path) -> None:
    """Return None and write nothing when no legacy file exists."""
    result = await legacy.apply(data_service.db, str(tmp_path), CUPS)
    assert result is None
    assert await data_service.get_supply() is None


@pytest.mark.asyncio
async def test_apply_is_idempotent(data_service: DataService, tmp_path) -> None:
    """Re-running apply does not duplicate rows (upsert)."""
    _install_legacy_file(str(tmp_path))
    await legacy.apply(data_service.db, str(tmp_path), CUPS)
    await legacy.apply(data_service.db, str(tmp_path), CUPS)

    assert len(await data_service.get_energy()) == 5
    assert len(await data_service.get_contracts()) == 2
    assert len(await data_service.get_power()) == 2
    assert len(await data_service.get_pvpc()) == 2


# --- orchestrator (DataService.run_migrations) ---


@pytest.mark.asyncio
async def test_run_migrations_compiles_statistics(
    data_service: DataService, tmp_path
) -> None:
    """run_migrations imports and compiles day/month statistics."""
    _install_legacy_file(str(tmp_path))
    results = await data_service.run_migrations(compile_statistics=True)

    assert [r.name for r in results] == ["legacy_json_1_3_3"]
    # Raw records imported and day/month statistics compiled immediately.
    assert len(await data_service.get_energy()) == 5
    assert len(await data_service.get_statistics("day")) > 0
    assert len(await data_service.get_statistics("month")) > 0


@pytest.mark.asyncio
async def test_run_migrations_can_skip_compilation(
    data_service: DataService, tmp_path
) -> None:
    """compile_statistics=False imports raw records without statistics."""
    _install_legacy_file(str(tmp_path))
    results = await data_service.run_migrations(compile_statistics=False)

    assert len(results) == 1
    assert len(await data_service.get_energy()) == 5
    assert await data_service.get_statistics("day") == []
    assert await data_service.get_statistics("month") == []


@pytest.mark.asyncio
async def test_run_migrations_no_legacy_file(data_service: DataService) -> None:
    """run_migrations is a no-op when there is no legacy file."""
    results = await data_service.run_migrations()
    assert results == []
