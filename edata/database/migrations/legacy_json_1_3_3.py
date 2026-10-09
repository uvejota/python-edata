"""Migration: import a python-edata 1.3.3 JSON storage export into the 2.0 DB.

1.x persisted everything as a single JSON file per supply at
``{storage_dir}/edata/edata_{cups}.json`` (ISO datetimes). 2.0 uses a SQLite
store. This migration imports the raw records only (supplies, contracts,
consumptions, maximeter, pvpc); the old ``*_sum`` aggregates are ignored because
2.0 recomputes daily/monthly statistics deterministically.

Self-contained on purpose: everything it needs lives here, so a future version
can drop this file (and its entry in the registry) once the upgrade window closes.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
import json
import logging
import os

from edata.database.controller import EdataDB
from edata.database.migrations.base import MigrationResult
from edata.models import Contract, Energy, Power, Supply
from edata.models.bill import EnergyPrice

_LOGGER = logging.getLogger(__name__)

NAME = "legacy_json_1_3_3"
LEGACY_SUBDIR = "edata"


def _legacy_path(storage_dir: str, cups: str) -> str:
    """Return the 1.3.3 storage file path for a supply."""

    return os.path.join(storage_dir, LEGACY_SUBDIR, f"edata_{cups.lower()}.json")


def _to_supply(d: dict) -> Supply:
    return Supply(
        cups=d["cups"],
        date_start=datetime.fromisoformat(d["date_start"]),
        date_end=datetime.fromisoformat(d["date_end"]),
        address=d.get("address"),
        postal_code=d.get("postal_code"),
        province=d.get("province"),
        municipality=d.get("municipality"),
        distributor=d.get("distributor"),
        point_type=d["pointType"],
        distributor_code=d["distributorCode"],
    )


def _to_contract(d: dict) -> Contract:
    power = [p for p in (d.get("power_p1"), d.get("power_p2")) if p is not None]
    return Contract(
        date_start=datetime.fromisoformat(d["date_start"]),
        date_end=datetime.fromisoformat(d["date_end"]),
        marketer=d["marketer"],
        distributor_code=d["distributorCode"],
        power=power,
    )


def _to_energy(d: dict) -> Energy:
    return Energy(
        datetime=datetime.fromisoformat(d["datetime"]),
        delta_h=d["delta_h"],
        consumption_kwh=d["value_kWh"],
        surplus_kwh=d.get("surplus_kWh", 0),
        real=d["real"],
    )


def _to_power(d: dict) -> Power:
    return Power(
        datetime=datetime.fromisoformat(d["datetime"]),
        value_kw=d["value_kW"],
    )


def _to_pvpc(d: dict) -> EnergyPrice:
    return EnergyPrice(
        datetime=datetime.fromisoformat(d["datetime"]),
        value_eur_kwh=d["value_eur_kWh"],
        delta_h=d["delta_h"],
    )


@dataclass
class _LegacyRecords:
    """Records parsed from a legacy JSON export."""

    supplies: list[Supply]
    contracts: list[Contract]
    energy: list[Energy]
    power: list[Power]
    pvpc: list[EnergyPrice]


def _load(path: str) -> _LegacyRecords | None:
    """Read and parse the legacy file; ``None`` if it does not exist.

    Blocking (file I/O plus building tens of thousands of records), so callers
    run it in a worker thread to keep the event loop free.
    """

    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return _LegacyRecords(
        supplies=[_to_supply(x) for x in raw.get("supplies", [])],
        contracts=[_to_contract(x) for x in raw.get("contracts", [])],
        energy=[_to_energy(x) for x in raw.get("consumptions", [])],
        power=[_to_power(x) for x in raw.get("maximeter", [])],
        pvpc=[_to_pvpc(x) for x in raw.get("pvpc", [])],
    )


async def apply(db: EdataDB, storage_dir: str, cups: str) -> MigrationResult | None:
    """Import a 1.3.3 JSON export. Returns None (no-op) if the file is absent."""

    path = _legacy_path(storage_dir, cups)
    records = await asyncio.to_thread(_load, path)
    if records is None:
        return None

    _LOGGER.info("Migrating legacy 1.3.3 storage from %s", path)
    result = MigrationResult(name=NAME)

    for supply in records.supplies:
        await db.add_supply(supply)
    result.supplies = len(records.supplies)

    # Contracts/energy/power reference a supply by cups; take it from the imported
    # supply record rather than the (lower-cased) filename id.
    target_cups = records.supplies[0].cups if records.supplies else cups

    for contract in records.contracts:
        await db.add_contract(target_cups, contract)
    result.contracts = len(records.contracts)

    await db.add_energy_list(target_cups, records.energy)
    result.energy = len(records.energy)

    await db.add_power_list(target_cups, records.power)
    result.power = len(records.power)

    await db.add_pvpc_list(records.pvpc)
    result.pvpc = len(records.pvpc)

    _LOGGER.info(
        "%s: imported %s supplies, %s contracts, %s energy, %s power, %s pvpc",
        NAME,
        result.supplies,
        result.contracts,
        result.energy,
        result.power,
        result.pvpc,
    )
    return result
