import typing
from datetime import datetime

from sqlmodel import asc, desc, select
from sqlmodel.sql.expression import SelectOfScalar

from edata.database.models import (
    BillModel,
    ContractModel,
    EnergyModel,
    PowerModel,
    PVPCModel,
    StatisticsModel,
    SupplyModel,
)


# Queries for "supply" table
def get_supply(cups: str) -> SelectOfScalar[SupplyModel]:
    """Query that selects a Supply."""

    return select(SupplyModel).where(SupplyModel.cups == cups)


def list_supply() -> SelectOfScalar[SupplyModel]:
    """Query that selects all Supply data."""

    return select(SupplyModel)


# Queries for "contract" table
def get_contract(
    cups: str, date_start: datetime | None = None
) -> SelectOfScalar[ContractModel]:
    """Query that selects a Contract."""

    query = select(ContractModel).where((ContractModel.cups == cups))
    if date_start:
        query = query.where(ContractModel.date_start == date_start)
    else:
        query = query.order_by(desc(ContractModel.date_start))

    return query


def list_contract(cups: str | None = None) -> SelectOfScalar[ContractModel]:
    """Query that selects all Contract data."""

    query = select(ContractModel)
    if cups is not None:
        query = query.where(ContractModel.cups == cups)
    query = query.order_by(asc(ContractModel.date_start))
    return query


# Queries for "energy" table
def get_energy(
    cups: str, datetime_: datetime | None = None
) -> SelectOfScalar[EnergyModel]:
    """Query that selects a Energy."""

    query = select(EnergyModel).where(EnergyModel.cups == cups)
    if datetime_ is not None:
        query = query.where(EnergyModel.datetime == datetime_)
    return query


def list_energy(
    cups: str, date_from: datetime | None = None, date_to: datetime | None = None
) -> SelectOfScalar[EnergyModel]:
    """Query that selects all Energy data."""

    query = select(EnergyModel).where(EnergyModel.cups == cups)
    if date_from:
        query = query.where(EnergyModel.datetime >= date_from)
    if date_to:
        query = query.where(EnergyModel.datetime <= date_to)
    query = query.order_by(asc(EnergyModel.datetime))
    return query


def get_last_energy(
    cups: str,
) -> SelectOfScalar[EnergyModel]:
    """Query that selects the most recent Energy record."""

    query = select(EnergyModel).where(EnergyModel.cups == cups)
    query = query.order_by(desc(EnergyModel.datetime))
    query = query.limit(1)

    return query


# Queries for "power" table
def get_power(
    cups: str, datetime_: datetime | None = None
) -> SelectOfScalar[PowerModel]:
    """Query that selects a power."""

    query = select(PowerModel).where(PowerModel.cups == cups)
    if datetime_ is not None:
        query = query.where(PowerModel.datetime == datetime_)
    return query


def list_power(
    cups: str, date_from: datetime | None = None, date_to: datetime | None = None
) -> SelectOfScalar[PowerModel]:
    """Query that selects all power data."""

    query = select(PowerModel).where(PowerModel.cups == cups)
    if date_from:
        query = query.where(PowerModel.datetime >= date_from)
    if date_to:
        query = query.where(PowerModel.datetime <= date_to)
    query = query.order_by(asc(PowerModel.datetime))
    return query


# Queries for "statistics" table
def get_statistics(
    cups: str, type_: typing.Literal["day", "month"], datetime_: datetime | None = None
) -> SelectOfScalar[StatisticsModel]:
    """Query that selects a statistics."""

    query = select(StatisticsModel).where(StatisticsModel.cups == cups)
    query = query.where(StatisticsModel.type == type_)
    if datetime_ is not None:
        query = query.where(StatisticsModel.datetime == datetime_)
    return query


def list_statistics(
    cups: str,
    type_: typing.Literal["day", "month"],
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    complete: bool | None = None,
) -> SelectOfScalar[StatisticsModel]:
    """Query that selects all statistics data."""

    query = select(StatisticsModel).where(StatisticsModel.cups == cups)
    query = query.where(StatisticsModel.type == type_)
    if date_from:
        query = query.where(StatisticsModel.datetime >= date_from)
    if date_to:
        query = query.where(StatisticsModel.datetime <= date_to)
    if complete is not None:
        query = query.where(StatisticsModel.complete == complete)
    query = query.order_by(asc(StatisticsModel.datetime))
    return query


def list_bill(
    cups: str,
    type_: typing.Literal["hour", "day", "month"],
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    complete: bool | None = None,
) -> SelectOfScalar[BillModel]:
    """Query that selects all bill data."""

    query = select(BillModel).where(BillModel.cups == cups)
    query = query.where(BillModel.type == type_)
    if date_from:
        query = query.where(BillModel.datetime >= date_from)
    if date_to:
        query = query.where(BillModel.datetime <= date_to)
    if complete is not None:
        query = query.where(BillModel.complete == complete)
    query = query.order_by(asc(BillModel.datetime))
    return query


def get_last_bill(
    cups: str,
) -> SelectOfScalar[BillModel]:
    """Query that selects the most recent bill record."""

    query = select(BillModel).where(BillModel.cups == cups)
    query = query.order_by(desc(BillModel.datetime))
    query = query.limit(1)

    return query


# Queries for "pvpc" table
def get_pvpc(datetime_: datetime) -> SelectOfScalar[PVPCModel]:
    """Query that selects a PVPC record."""

    query = select(PVPCModel).where(PVPCModel.datetime == datetime_)
    return query


def get_last_pvpc() -> SelectOfScalar[PVPCModel]:
    """Query that selects the most recent pvpc record."""

    query = select(PVPCModel)
    query = query.order_by(desc(PVPCModel.datetime))
    query = query.limit(1)

    return query


def list_pvpc(
    date_from: datetime | None = None, date_to: datetime | None = None
) -> SelectOfScalar[PVPCModel]:
    """Query that selects a PVPC record."""

    query = select(PVPCModel)
    if date_from:
        query = query.where(PVPCModel.datetime >= date_from)
    if date_to:
        query = query.where(PVPCModel.datetime <= date_to)
    return query


def get_bill(cups, type_, datetime_):
    """Query that selects a bill."""

    query = select(BillModel).where(BillModel.cups == cups)
    query = query.where(BillModel.type == type_)
    if datetime_ is not None:
        query = query.where(BillModel.datetime == datetime_)
    return query
