import asyncio
import functools
import logging
import os
import typing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from sqlalchemy import Engine, Select, Table, create_engine, event, insert, or_
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, SQLModel, UniqueConstraint
from sqlmodel.sql.expression import SelectOfScalar

import edata.database.queries as q
from edata.database.models import (
    BillModel,
    ContractModel,
    EnergyModel,
    PowerModel,
    PVPCModel,
    StatisticsModel,
    SupplyModel,
)
from edata.models import Contract, Energy, Power, Statistics, Supply
from edata.models.bill import Bill, EnergyPrice

_LOGGER = logging.getLogger(__name__)

T = typing.TypeVar("T", bound=SQLModel)
R = typing.TypeVar("R")


def _set_sqlite_pragmas(dbapi_connection, connection_record) -> None:
    """Tune SQLite for this write-heavy, single-writer workload.

    WAL lets readers proceed while a write is in flight and avoids an fsync per
    statement; synchronous=NORMAL is durable under WAL (only a crash mid-checkpoint
    could lose the last transaction, and the data is re-fetchable from Datadis);
    busy_timeout avoids spurious "database is locked" errors under concurrency.
    journal_mode is persisted in the file; the others are per-connection.
    """

    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=5000")
    finally:
        cursor.close()


def _create_missing_indexes(connection) -> None:
    """Create indexes added after a table was first created.

    ``create_all`` skips tables that already exist, indexes included, so
    databases created by an older version would never get new indexes.
    """

    for table in SQLModel.metadata.sorted_tables:
        for index in table.indexes:
            index.create(connection, checkfirst=True)


def _conflict_columns(table: Table) -> list[str]:
    """Return the columns that identify a row for upserts on ``table``.

    That is the table's unique constraint, else its unique index, else its
    primary key.
    """

    for constraint in table.constraints:
        if isinstance(constraint, UniqueConstraint):
            return [c.name for c in constraint.columns]
    for index in table.indexes:
        if index.unique:
            return [c.name for c in index.columns]
    return [c.name for c in table.primary_key.columns]


def _in_db_thread(
    fn: typing.Callable[..., R],
) -> typing.Callable[..., typing.Coroutine[typing.Any, typing.Any, R]]:
    """Turn a blocking ``EdataDB`` method into a coroutine run on its DB thread."""

    @functools.wraps(fn)
    async def wrapper(self: "EdataDB", *args: typing.Any, **kwargs: typing.Any) -> R:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, functools.partial(fn, self, *args, **kwargs)
        )

    return wrapper


class EdataDB:
    """SQLite store.

    The public API is async, but every database operation (SQLAlchemy, pydantic
    (de)serialization and SQLite itself) runs on a single dedicated thread so a
    large import never stalls the caller's event loop (e.g. Home Assistant's).
    One thread also serializes access, which suits SQLite's single writer.
    """

    _instance = None
    _engine: Engine | None = None
    _db_url: str | None = None

    def __new__(cls, sqlite_path: str):
        db_url = f"sqlite:////{os.path.abspath(sqlite_path)}"
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._db_url = db_url
            # Ensure parent directory exists before the first connection is opened.
            dir_path = os.path.dirname(os.path.abspath(sqlite_path))
            os.makedirs(dir_path, exist_ok=True)
            cls._engine = create_engine(db_url)
            event.listen(cls._engine, "connect", _set_sqlite_pragmas)
            cls._instance._tables_initialized = False
            cls._instance._executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="edata-db"
            )
        elif db_url != cls._db_url:
            raise ValueError("EdataDB already initialized with a different db_url")
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Close the shared instance (its DB thread and engine) and forget it.

        Waits for queued database work to finish first. A later ``EdataDB(...)``
        starts afresh, possibly on another path.
        """

        if cls._instance is not None:
            cls._instance._executor.shutdown(wait=True)
        if cls._engine is not None:
            cls._engine.dispose()
        cls._instance = None
        cls._engine = None
        cls._db_url = None

    @property
    def engine(self) -> Engine | None:
        """Return the database engine."""

        return self._engine

    def _ensure_tables(self) -> None:
        """Create tables and missing indexes if not already done (lazy init).

        Only ever runs on the database thread, so concurrent first calls are
        serialized and cannot race to create the same index.
        """

        if self._tables_initialized or not self.engine:
            return
        with self.engine.begin() as conn:
            SQLModel.metadata.create_all(conn)
            _create_missing_indexes(conn)
        self._tables_initialized = True

    def _add_one(
        self,
        session: Session,
        record: T,
        commit: bool = True,
    ) -> T:
        """Add a single record into the database."""

        session.add(record)
        if commit:
            session.commit()
            session.refresh(record)
        else:
            session.flush()
        return record

    def _update_one(
        self,
        session: Session,
        query: SelectOfScalar,
        data: typing.Any,
        commit: bool = True,
        overrides: dict[str, typing.Any] | None = None,
    ) -> T | None:  # type: ignore
        """Update a single record in the database."""

        result = session.exec(query)
        existing = result.first()
        if existing and getattr(existing, "data") == data and not overrides:
            return existing
        setattr(existing, "data", data)
        if overrides:
            for key, value in overrides.items():
                setattr(existing, key, value)
        if commit:
            session.commit()
            session.refresh(existing)
        else:
            session.flush()
        return existing

    def _add_or_update_one(
        self,
        session: Session,
        query: SelectOfScalar,
        record: T,
        commit: bool = True,
        override: list[str] | None = None,
    ) -> T | None:
        """Add a single record into the database and fallback to update safely."""

        try:
            with session.begin_nested():
                session.add(record)
                session.flush()
            if commit:
                session.commit()
                session.refresh(record)
            return record
        except IntegrityError:
            new_data = getattr(record, "data")
            override_dict = None
            record_json = record.model_dump()
            if override:
                override_dict = {
                    x: record_json[x] for x in record.model_dump() if x in override
                }
            return self._update_one(
                session, query, new_data, commit=commit, overrides=override_dict
            )

    def _add_or_update_many(
        self,
        session: Session,
        model: type[SQLModel],
        rows: list[dict[str, typing.Any]],
        override: list[str] | None = None,
    ) -> None:
        """Insert many rows, updating the existing ones, in a single statement.

        Uses SQLite's ``INSERT ... ON CONFLICT DO UPDATE`` so re-syncing rows that
        are already stored costs one bulk statement instead of a savepoint, a
        failed insert and a lookup per row. Existing rows are only rewritten (and
        their ``updated_at`` bumped) when ``data`` or an ``override`` column
        actually changed.

        Rows are plain column dicts: building an ORM instance per row only to
        read it back cost about half of a full-history import. Columns missing
        from a row take the model defaults, resolved once per batch.
        """
        if not rows:
            return

        table = model.__table__  # type: ignore[attr-defined]
        defaults = {
            c.name: model.model_fields[c.name].get_default(call_default_factory=True)
            for c in table.columns
            if c.name != "id" and c.name not in rows[0]
        }
        rows = [{**defaults, **row} for row in rows]

        updated = ["data", *(override or [])]
        stmt = sqlite_insert(table)
        stmt = stmt.on_conflict_do_update(
            index_elements=_conflict_columns(table),
            set_={
                **{c: stmt.excluded[c] for c in updated},
                "updated_at": stmt.excluded.updated_at,
            },
            where=or_(*(table.c[c].is_distinct_from(stmt.excluded[c]) for c in updated)),
        )
        connection = session.connection()
        connection.execute(stmt, rows)
        session.commit()

    @_in_db_thread
    def get_supply(self, cups: str) -> SupplyModel | None:
        """Get a supply record by cups."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.get_supply(cups))
            return result.first()

    @_in_db_thread
    def get_contract(
        self, cups: str, date_start: datetime | None = None
    ) -> ContractModel | None:
        """Get a contract record by cups."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.get_contract(cups, date_start))
            return result.first()

    @_in_db_thread
    def get_last_energy(self, cups: str) -> EnergyModel | None:
        """Get the most recent Energy record by cups."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.get_last_energy(cups))
            return result.first()

    @_in_db_thread
    def get_last_power(self, cups: str) -> PowerModel | None:
        """Get the most recent power record by cups."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.get_last_power(cups))
            return result.first()

    @_in_db_thread
    def get_last_pvpc(self) -> PVPCModel | None:
        """Get the most recent pvpc."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.get_last_pvpc())
            return result.first()

    @_in_db_thread
    def get_last_bill(self, cups: str) -> BillModel | None:
        """Get the most recent bill record by cups."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.get_last_bill(cups))
            return result.first()

    @_in_db_thread
    def get_last_complete_statistic(
        self, cups: str, type_: typing.Literal["day", "month"]
    ) -> StatisticsModel | None:
        """Get the most recent complete statistics record by cups and type."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.get_last_complete_statistic(cups, type_))
            return result.first()

    @_in_db_thread
    def get_last_complete_bill(
        self, cups: str, type_: typing.Literal["hour", "day", "month"]
    ) -> BillModel | None:
        """Get the most recent complete bill record by cups and type."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.get_last_complete_bill(cups, type_))
            return result.first()

    @_in_db_thread
    def add_contract(self, cups: str, contract: Contract) -> ContractModel | None:
        """Add or update a contract record."""

        self._ensure_tables()
        record = ContractModel(cups=cups, date_start=contract.date_start, data=contract)
        with Session(self.engine) as session:
            return self._add_or_update_one(
                session, q.get_contract(cups, contract.date_start), record
            )

    @_in_db_thread
    def add_supply(self, supply: Supply) -> SupplyModel | None:
        """Add or update a supply record."""

        self._ensure_tables()
        record = SupplyModel(cups=supply.cups, data=supply)
        with Session(self.engine) as session:
            return self._add_or_update_one(
                session, q.get_supply(supply.cups), record
            )

    @_in_db_thread
    def add_energy(self, cups: str, energy: Energy) -> EnergyModel | None:
        """Add or update an energy record."""

        self._ensure_tables()
        record = EnergyModel(
            cups=cups, delta_h=energy.delta_h, datetime=energy.datetime, data=energy
        )
        with Session(self.engine) as session:
            return self._add_or_update_one(
                session, q.get_energy(cups, energy.datetime), record
            )

    @_in_db_thread
    def add_energy_list(self, cups: str, energy: list[Energy]) -> None:
        """Add or update a list of energy records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            unique_map = {item.datetime: item for item in energy}
            unique = list(unique_map.values())
            rows = [
                {"cups": cups, "delta_h": x.delta_h, "datetime": x.datetime, "data": x}
                for x in unique
            ]
            self._add_or_update_many(session, EnergyModel, rows)

    @_in_db_thread
    def add_power(self, cups: str, power: Power) -> PowerModel | None:
        """Add or update a power record for a given CUPS and Power instance."""

        self._ensure_tables()
        record = PowerModel(cups=cups, datetime=power.datetime, data=power)
        with Session(self.engine) as session:
            return self._add_or_update_one(
                session, q.get_power(cups, power.datetime), record
            )

    @_in_db_thread
    def add_power_list(self, cups: str, power: list[Power]) -> None:
        """Add or update a list of power records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            unique_map = {item.datetime: item for item in power}
            unique = list(unique_map.values())
            rows = [{"cups": cups, "datetime": x.datetime, "data": x} for x in unique]
            self._add_or_update_many(session, PowerModel, rows)

    @_in_db_thread
    def add_pvpc(self, pvpc: EnergyPrice) -> PVPCModel | None:
        """Add or update a pvpc record."""

        self._ensure_tables()
        record = PVPCModel(datetime=pvpc.datetime, data=pvpc)
        with Session(self.engine) as session:
            return self._add_or_update_one(
                session, q.get_pvpc(pvpc.datetime), record
            )

    @_in_db_thread
    def add_pvpc_list(self, pvpc: list[EnergyPrice]) -> None:
        """Add or update a list of pvpc records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            unique_map = {item.datetime: item for item in pvpc}
            unique = list(unique_map.values())
            rows = [{"datetime": x.datetime, "data": x} for x in unique]
            self._add_or_update_many(session, PVPCModel, rows)

    @_in_db_thread
    def add_statistics(
        self,
        cups: str,
        type_: typing.Literal["day", "month"],
        data: Statistics,
        complete: bool = False,
    ) -> StatisticsModel | None:
        """Add or update a statistics record."""

        self._ensure_tables()
        record = StatisticsModel(
            cups=cups, datetime=data.datetime, type=type_, data=data, complete=complete
        )
        with Session(self.engine) as session:
            return self._add_or_update_one(
                session,
                q.get_statistics(cups, type_, data.datetime),
                record,
                override=["complete"],
            )

    @_in_db_thread
    def add_statistics_list(
        self,
        cups: str,
        type_: typing.Literal["day", "month"],
        complete: bool,
        statistics: list[Statistics],
    ) -> None:
        """Add or update a list of statistics records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            unique_map = {item.datetime: item for item in statistics}
            rows = [
                {
                    "cups": cups,
                    "datetime": x.datetime,
                    "type": type_,
                    "complete": complete,
                    "data": x,
                }
                for x in unique_map.values()
            ]
            self._add_or_update_many(
                session, StatisticsModel, rows, override=["complete"]
            )

    @_in_db_thread
    def add_bill(
        self,
        cups: str,
        type_: typing.Literal["hour", "day", "month"],
        data: Bill,
        confhash: str,
        complete: bool,
    ) -> BillModel | None:
        """Add or update a bill record."""

        self._ensure_tables()
        record = BillModel(
            cups=cups,
            datetime=data.datetime,
            type=type_,
            complete=complete,
            confhash=confhash,
            data=data,
        )
        with Session(self.engine) as session:
            return self._add_or_update_one(
                session,
                q.get_bill(cups, type_, data.datetime),
                record,
                override=["complete", "confhash"],
            )

    @_in_db_thread
    def add_bill_list(
        self,
        cups: str,
        type_: typing.Literal["hour", "day", "month"],
        confhash: str,
        complete: bool,
        bill: list[Bill],
    ) -> None:
        """Add or update a list of bill records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            unique_map = {item.datetime: item for item in bill}
            unique = list(unique_map.values())
            rows = [
                {
                    "cups": cups,
                    "datetime": x.datetime,
                    "type": type_,
                    "confhash": confhash,
                    "complete": complete,
                    "data": x,
                }
                for x in unique
            ]
            self._add_or_update_many(
                session, BillModel, rows, override=["complete", "confhash"]
            )

    @_in_db_thread
    def clear_bills(self, cups: str, since: datetime | None = None) -> None:
        """Delete bill records for a cups, optionally only from a datetime onwards."""

        self._ensure_tables()
        with Session(self.engine) as session:
            session.exec(q.delete_bill(cups, since))  # type: ignore[call-overload]
            session.commit()

    @_in_db_thread
    def list_supplies(self) -> typing.Sequence[SupplyModel]:
        """List all supply records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.list_supply())
            return result.all()

    @_in_db_thread
    def list_contracts(
        self, cups: str | None = None
    ) -> typing.Sequence[ContractModel]:
        """List all contract records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.list_contract(cups))
            return result.all()

    def _list_data(self, query: SelectOfScalar, model: type[SQLModel]) -> list:
        """Return only the ``data`` payload of the rows selected by ``query``.

        Skips building an ORM instance per row, which nearly halves the cost of
        reading a month of hourly records.
        """

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(
                query.with_only_columns(model.data)  # type: ignore[attr-defined]
            )
            return list(result.all())

    @_in_db_thread
    def list_energy_data(
        self,
        cups: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[Energy]:
        """List the energy data (without row metadata)."""

        return self._list_data(
            q.list_energy(cups, date_from, date_to), EnergyModel
        )

    @_in_db_thread
    def list_pvpc_data(
        self,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[EnergyPrice]:
        """List the pvpc data (without row metadata)."""

        return self._list_data(q.list_pvpc(date_from, date_to), PVPCModel)

    @_in_db_thread
    def list_bill_data(
        self,
        cups: str,
        type_: typing.Literal["hour", "day", "month"],
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[Bill]:
        """List the bill data (without row metadata)."""

        return self._list_data(
            q.list_bill(cups, type_, date_from, date_to), BillModel
        )

    @_in_db_thread
    def list_energy(
        self,
        cups: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> typing.Sequence[EnergyModel]:
        """List energy records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.list_energy(cups, date_from, date_to))
            return result.all()

    @_in_db_thread
    def list_power(
        self,
        cups: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> typing.Sequence[PowerModel]:
        """List power records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.list_power(cups, date_from, date_to))
            return result.all()

    @_in_db_thread
    def list_pvpc(
        self,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> typing.Sequence[PVPCModel]:
        """List pvpc records."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(q.list_pvpc(date_from, date_to))
            return result.all()

    @_in_db_thread
    def list_statistics(
        self,
        cups: str,
        type_: typing.Literal["day", "month"],
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        complete: bool | None = None,
    ) -> typing.Sequence[StatisticsModel]:
        """List statistics records filtered by type ('day' or 'month') and date range."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(
                q.list_statistics(cups, type_, date_from, date_to, complete)
            )
            return result.all()

    @_in_db_thread
    def list_bill(
        self,
        cups: str,
        type_: typing.Literal["hour", "day", "month"],
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        complete: bool | None = None,
    ) -> typing.Sequence[BillModel]:
        """List bill records filtered by type ('hour', 'day' or 'month') and date range."""

        self._ensure_tables()
        with Session(self.engine) as session:
            result = session.exec(
                q.list_bill(cups, type_, date_from, date_to, complete)
            )
            return result.all()
