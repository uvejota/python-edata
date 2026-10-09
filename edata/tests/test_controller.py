"""Tests for the database controller."""

import asyncio
import threading
from collections.abc import AsyncIterator
from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import DateTime
from sqlmodel import SQLModel

from edata.database.controller import EdataDB
from edata.models import Bill, Energy, Supply

CUPS = "ESXXXXXXXXXXXXXXXXTEST"
START = datetime(2024, 1, 1)


@pytest_asyncio.fixture
async def db(tmp_path) -> AsyncIterator[EdataDB]:
    """An EdataDB on an isolated on-disk database with one supply."""
    EdataDB.reset()
    database = EdataDB(str(tmp_path / "edata.db"))
    await database.add_supply(
        Supply(
            cups=CUPS,
            date_start=START,
            date_end=START + timedelta(days=30),
            address=None,
            postal_code=None,
            province=None,
            municipality=None,
            distributor=None,
            point_type=5,
            distributor_code="2",
        )
    )
    yield database
    EdataDB.reset()


def _energy(hours: int, kwh: float) -> list[Energy]:
    return [
        Energy(
            datetime=START + timedelta(hours=h),
            delta_h=1.0,
            consumption_kwh=kwh,
            real=True,
        )
        for h in range(hours)
    ]


@pytest.mark.asyncio
async def test_add_energy_list_upserts(db: EdataDB) -> None:
    await db.add_energy_list(CUPS, _energy(24, 0.1))
    before = {x.datetime: x for x in await db.list_energy(CUPS)}

    # overlapping batch: first 12 hours unchanged, last 12 changed, 12 new
    await db.add_energy_list(CUPS, _energy(12, 0.1) + _energy(36, 0.5)[12:])
    after = {x.datetime: x for x in await db.list_energy(CUPS)}

    assert len(after) == 36
    for h in range(36):
        dt = START + timedelta(hours=h)
        assert after[dt].data.consumption_kwh == (0.1 if h < 12 else 0.5)
        if h < 12:
            # unchanged rows are not rewritten
            assert after[dt].id == before[dt].id
            assert after[dt].updated_at == before[dt].updated_at
        elif h < 24:
            assert after[dt].id == before[dt].id
            assert after[dt].updated_at > before[dt].updated_at


@pytest.mark.asyncio
async def test_add_bill_list_applies_overrides(db: EdataDB) -> None:
    bills = [Bill(datetime=START + timedelta(hours=h), delta_h=1) for h in range(3)]
    await db.add_bill_list(CUPS, "hour", "hash-a", False, bills)

    # same data, only the override columns change
    await db.add_bill_list(CUPS, "hour", "hash-b", True, bills)

    stored = await db.list_bill(CUPS, "hour")
    assert len(stored) == 3
    assert all(x.complete and x.confhash == "hash-b" for x in stored)


@pytest.mark.asyncio
async def test_concurrent_first_calls_do_not_race_index_creation(db: EdataDB) -> None:
    # simulate a database created before the index existed, opened fresh
    with db.engine.begin() as conn:
        conn.exec_driver_sql("DROP INDEX ix_energy_cups_datetime")
    db._tables_initialized = False

    await asyncio.gather(*(db.get_last_energy(CUPS) for _ in range(10)))

    with db.engine.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE name='ix_energy_cups_datetime'"
        )
        assert result.first() is not None


def test_datetime_columns_are_naive() -> None:
    # the supply timezone is unknown, so datetimes are stored as-is (naive);
    # plain ``datetime`` fields map to UTC-aware columns on sqlmodel>=0.0.45
    columns = [
        column
        for table in SQLModel.metadata.sorted_tables
        for column in table.columns
        if isinstance(column.type, DateTime)
    ]
    assert len(columns) == 20
    assert all(column.type.timezone is False for column in columns)


@pytest.mark.asyncio
async def test_reset_stops_db_thread_and_allows_new_path(db: EdataDB, tmp_path) -> None:
    assert await db.list_supplies()
    EdataDB.reset()

    assert not any(t.name.startswith("edata-db") for t in threading.enumerate())
    other = EdataDB(str(tmp_path / "other.db"))
    assert other is not db
    assert await other.list_supplies() == []
