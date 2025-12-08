"""Collection of utilities."""

import logging
from datetime import datetime
from pathlib import Path

import holidays

from edata.models.supply import Contract

TARIFF_BY_HOUR = [
    [10, 11, 12, 13, 18, 19, 20, 21],  # p1
    [8, 9, 14, 15, 16, 17, 22, 23],  # p2
    [0, 1, 2, 3, 4, 5, 6, 7],  # p3
]

TARIFF_BY_WEEKDAY = [[], [], [5, 6]]

_LOGGER = logging.getLogger(__name__)


def get_tariff(dt: datetime) -> int:
    """Return the tariff for the selected datetime."""

    hdays = holidays.country_holidays("ES")
    hour = dt.hour
    weekday = dt.weekday()

    if dt.date() in hdays:
        # holidays are p3
        return 3

    for idx, weekdays in enumerate(TARIFF_BY_WEEKDAY):
        if weekday in weekdays:
            return idx + 1

    for idx, hours in enumerate(TARIFF_BY_HOUR):
        if hour in hours:
            return idx + 1

    # we shouldn't get here
    _LOGGER.error("Cannot decide the tariff for %s", dt.isoformat())
    return 0


def get_contract_for_dt(contracts: list[Contract], date: datetime):
    """Return the active contract for a provided datetime."""

    for contract in contracts:
        if contract.date_start <= date <= contract.date_end:
            return contract
    return None


def get_month(dt: datetime) -> datetime:
    """Return a datetime that represents the month start for a provided datetime."""

    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def get_day(dt: datetime) -> datetime:
    """Return a datetime that represents the day start for a provided datetime."""

    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def redacted_cups(cups: str) -> str:
    """Return an anonymized version of the cups identifier."""

    return cups[-5:]


def get_db_path(storage_dir: str) -> str:
    """Return the database path for a given root storage dir."""

    return str(Path(storage_dir).absolute() / "edata.db")
