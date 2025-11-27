import logging
import os
import typing
from datetime import datetime

from sqlalchemy import Select, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
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


class EdataDB:

    _instance = None
    _engine: AsyncEngine | None = None
    _db_url: str | None = None

    def __new__(cls, sqlite_path: str):
        db_url = f"sqlite+aiosqlite:////{os.path.abspath(sqlite_path)}"
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._db_url = db_url
            cls._engine = create_async_engine(db_url, future=True)
            # Ensure parent directory exists
            dir_path = os.path.dirname(os.path.abspath(sqlite_path))
            os.makedirs(dir_path, exist_ok=True)
            cls._instance._tables_initialized = False
        elif db_url != cls._db_url:
            raise ValueError("EdataDB already initialized with a different db_url")
        return cls._instance

    @property
    def engine(self):
        """Return the async database engine."""

        return self._engine

    async def _ensure_tables(self):
        """Create tables if not already created (lazy init)."""

        if self._tables_initialized:
            return
        if self.engine:
            async with self.engine.begin() as conn:
                await conn.run_sync(SQLModel.metadata.create_all)
            self._tables_initialized = True

    async def _add_one(
        self,
        session: AsyncSession,
        record: T,
        commit: bool = True,
    ) -> T:
        """Add a single record into the database."""

        session.add(record)
        if commit:
            await session.commit()
            await session.refresh(record)
        else:
            await session.flush()
        return record

    async def _update_one(
        self,
        session: AsyncSession,
        query: SelectOfScalar,
        data,
        commit: bool = True,
        overrides: dict[str, typing.Any] | None = None,
    ) -> T | None:  # type: ignore
        """Updates a single record in the database."""

        result = await session.exec(query)
        existing = result.first()
        if existing and getattr(existing, "data") == data and not overrides:
            return existing
        setattr(existing, "data", data)
        if overrides:
            for key, value in overrides.items():
                setattr(existing, key, value)
        if commit:
            await session.commit()
            await session.refresh(existing)
        else:
            await session.flush()
        return existing

    async def _add_or_update_one(
        self,
        session: AsyncSession,
        query: SelectOfScalar,
        record: T,
        commit: bool = True,
        override: list[str] | None = None,
    ) -> T | None:
        """Add a single record into the database and fallback to update safely."""

        try:
            async with session.begin_nested():
                session.add(record)
                await session.flush()
            if commit:
                await session.commit()
                await session.refresh(record)
            return record
        except IntegrityError:
            new_data = getattr(record, "data")
            override_dict = None
            record_json = record.model_dump()
            if override:
                override_dict = {
                    x: record_json[x] for x in record.model_dump() if x in override
                }
            return await self._update_one(
                session, query, new_data, commit=commit, overrides=override_dict
            )

    async def _add_or_update_many(
        self,
        session: AsyncSession,
        queries: list[SelectOfScalar],
        records: list[T],
        batch_size: int = 100,
        override: list[str] | None = None,
    ) -> list[T]:
        """Updates many records in the database"""
        if not records:
            return []

        for i in range(0, len(records), batch_size):
            chunk_records = records[i : i + batch_size]
            chunk_queries = queries[i : i + batch_size]
            try:
                async with session.begin_nested():
                    session.add_all(chunk_records)
                    await session.flush()
            except IntegrityError:
                for j, record in enumerate(chunk_records):
                    await self._add_or_update_one(
                        session,
                        chunk_queries[j],
                        record,
                        commit=False,
                        override=override,
                    )
        await session.commit()
        return records

    async def get_supply(self, cups: str) -> SupplyModel | None:
        """Get a supply record by cups."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.get_supply(cups))
            return result.first()

    async def get_contract(
        self, cups: str, date_start: datetime | None = None
    ) -> ContractModel | None:
        """Get a contract record by cups."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.get_contract(cups, date_start))
            return result.first()

    async def get_last_energy(self, cups: str) -> EnergyModel | None:
        """Get the most recent Energy record by cups."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.get_last_energy(cups))
            return result.first()

    async def get_last_pvpc(self) -> PVPCModel | None:
        """Get the most recent pvpc."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.get_last_pvpc())
            return result.first()

    async def get_last_bill(self, cups: str) -> BillModel | None:
        """Get the most recent bill record by cups."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.get_last_bill(cups))
            return result.first()

    async def add_contract(self, cups: str, contract: Contract) -> ContractModel | None:
        """Add or update a contract record."""

        await self._ensure_tables()
        record = ContractModel(cups=cups, date_start=contract.date_start, data=contract)
        async with AsyncSession(self.engine) as session:
            return await self._add_or_update_one(
                session, q.get_contract(cups, contract.date_start), record
            )

    async def add_supply(self, supply: Supply) -> SupplyModel | None:
        """Add or update a supply record."""

        await self._ensure_tables()
        record = SupplyModel(cups=supply.cups, data=supply)
        async with AsyncSession(self.engine) as session:
            return await self._add_or_update_one(
                session, q.get_supply(supply.cups), record
            )

    async def add_energy(self, cups: str, energy: Energy) -> EnergyModel | None:
        """Add or update an energy record."""

        await self._ensure_tables()
        record = EnergyModel(
            cups=cups, delta_h=energy.delta_h, datetime=energy.datetime, data=energy
        )
        async with AsyncSession(self.engine) as session:
            return await self._add_or_update_one(
                session, q.get_energy(cups, energy.datetime), record
            )

    async def add_energy_list(self, cups: str, energy: list[Energy]):
        """Add or update a list of energy records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            unique_map = {item.datetime: item for item in energy}
            unique = list(unique_map.values())
            queries = [q.get_energy(cups, x.datetime) for x in unique]
            items = [
                EnergyModel(cups=cups, delta_h=x.delta_h, datetime=x.datetime, data=x)
                for x in unique
            ]
            await self._add_or_update_many(session, queries, items)

    async def add_power(self, cups: str, power: Power) -> PowerModel | None:
        """Add or update a power record for a given CUPS and Power instance."""

        await self._ensure_tables()
        record = PowerModel(cups=cups, datetime=power.datetime, data=power)
        async with AsyncSession(self.engine) as session:
            return await self._add_or_update_one(
                session, q.get_power(cups, power.datetime), record
            )

    async def add_power_list(self, cups: str, power: list[Power]):
        """Add or update a list of power records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            unique_map = {item.datetime: item for item in power}
            unique = list(unique_map.values())
            queries = [q.get_power(cups, x.datetime) for x in unique]
            items = [PowerModel(cups=cups, datetime=x.datetime, data=x) for x in unique]
            await self._add_or_update_many(session, queries, items)

    async def add_pvpc(self, pvpc: EnergyPrice) -> PVPCModel | None:
        """Add or update a pvpc record."""

        await self._ensure_tables()
        record = PVPCModel(datetime=pvpc.datetime, data=pvpc)
        async with AsyncSession(self.engine) as session:
            return await self._add_or_update_one(
                session, q.get_pvpc(pvpc.datetime), record
            )

    async def add_pvpc_list(self, pvpc: list[EnergyPrice]):
        """Add or update a list of pvpc records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            unique_map = {item.datetime: item for item in pvpc}
            unique = list(unique_map.values())
            queries = [q.get_pvpc(x.datetime) for x in unique]
            items = [PVPCModel(datetime=x.datetime, data=x) for x in unique]
            await self._add_or_update_many(session, queries, items)

    async def add_statistics(
        self,
        cups: str,
        type_: typing.Literal["day", "month"],
        data: Statistics,
        complete: bool = False,
    ) -> StatisticsModel | None:
        """Add or update a statistics record."""

        await self._ensure_tables()
        record = StatisticsModel(
            cups=cups, datetime=data.datetime, type=type_, data=data, complete=complete
        )
        async with AsyncSession(self.engine) as session:
            return await self._add_or_update_one(
                session,
                q.get_statistics(cups, type_, data.datetime),
                record,
                override=["complete"],
            )

    async def add_bill(
        self,
        cups: str,
        type_: typing.Literal["hour", "day", "month"],
        data: Bill,
        confhash: str,
        complete: bool,
    ) -> BillModel | None:
        """Add or update a bill record."""

        await self._ensure_tables()
        record = BillModel(
            cups=cups,
            datetime=data.datetime,
            type=type_,
            complete=complete,
            confhash=confhash,
            data=data,
        )
        async with AsyncSession(self.engine) as session:
            return await self._add_or_update_one(
                session,
                q.get_bill(cups, type_, data.datetime),
                record,
                override=["complete", "confhash"],
            )

    async def add_bill_list(
        self,
        cups: str,
        type_: typing.Literal["hour", "day", "month"],
        confhash: str,
        complete: bool,
        bill: list[Bill],
    ):
        """Add or update a list of bill records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            unique_map = {item.datetime: item for item in bill}
            unique = list(unique_map.values())
            queries = [q.get_bill(cups, type_, x.datetime) for x in unique]
            items = [
                BillModel(
                    cups=cups,
                    datetime=x.datetime,
                    type=type_,
                    confhash=confhash,
                    complete=complete,
                    data=x,
                )
                for x in unique
            ]
            await self._add_or_update_many(
                session, queries, items, override=["complete", "confhash"]
            )

    async def list_supplies(self):
        """List all supply records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.list_supply())
            return result.all()

    async def list_contracts(self, cups: str | None = None):
        """List all contract records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.list_contract(cups))
            return result.all()

    async def list_energy(
        self,
        cups: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ):
        """List energy records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.list_energy(cups, date_from, date_to))
            return result.all()

    async def list_power(
        self,
        cups: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ):
        """List power records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.list_power(cups, date_from, date_to))
            return result.all()

    async def list_pvpc(
        self,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ):
        """List pvpc records."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(q.list_pvpc(date_from, date_to))
            return result.all()

    async def list_statistics(
        self,
        cups: str,
        type_: typing.Literal["day", "month"],
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        complete: bool | None = None,
    ):
        """List statistics records filtered by type ('day' or 'month') and date range."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(
                q.list_statistics(cups, type_, date_from, date_to, complete)
            )
            return result.all()

    async def list_bill(
        self,
        cups: str,
        type_: typing.Literal["hour", "day", "month"],
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        complete: bool | None = None,
    ):
        """List bill records filtered by type ('hour', 'day' or 'month') and date range."""

        await self._ensure_tables()
        async with AsyncSession(self.engine) as session:
            result = await session.exec(
                q.list_bill(cups, type_, date_from, date_to, complete)
            )
            return result.all()
