import typing
from datetime import datetime as dt

from sqlmodel import AutoString, Column, Field, SQLModel, UniqueConstraint

from edata.database.utils import PydanticJSON
from edata.models import Bill, Contract, Energy, EnergyPrice, Power, Statistics, Supply


class SupplyModel(SQLModel, table=True):

    __tablename__ = "supply"  # type: ignore

    __table_args__ = {"extend_existing": True}

    cups: str = Field(default=None, primary_key=True)
    data: Supply = Field(sa_column=Column(PydanticJSON(Supply)))
    version: int = Field(default=1)
    created_at: dt = Field(default_factory=dt.now, nullable=False)
    updated_at: dt = Field(
        default_factory=dt.now, nullable=False, sa_column_kwargs={"onupdate": dt.now}
    )


class ContractModel(SQLModel, table=True):

    __tablename__ = "contract"  # type: ignore

    __table_args__ = (
        UniqueConstraint("cups", "date_start", name="uq_contract_cups_start"),
        {"extend_existing": True},
    )

    id: int | None = Field(default=None, primary_key=True)
    cups: str = Field(foreign_key="supply.cups", index=True)
    date_start: dt = Field(index=True)
    data: Contract = Field(sa_column=Column(PydanticJSON(Contract)))

    version: int = Field(default=1)
    created_at: dt = Field(default_factory=dt.now, nullable=False)
    updated_at: dt = Field(
        default_factory=dt.now, nullable=False, sa_column_kwargs={"onupdate": dt.now}
    )


class EnergyModel(SQLModel, table=True):

    __tablename__ = "energy"  # type: ignore

    __table_args__ = (
        UniqueConstraint(
            "cups", "delta_h", "datetime", name="uq_energy_cups_delta_datetime"
        ),
        {"extend_existing": True},
    )

    id: int | None = Field(default=None, primary_key=True)
    cups: str = Field(foreign_key="supply.cups", index=True)
    delta_h: float
    datetime: dt = Field(index=True)

    data: Energy = Field(sa_column=Column(PydanticJSON(Energy)))

    version: int = Field(default=1)
    created_at: dt = Field(default_factory=dt.now, nullable=False)
    updated_at: dt = Field(
        default_factory=dt.now, nullable=False, sa_column_kwargs={"onupdate": dt.now}
    )


class PowerModel(SQLModel, table=True):

    __tablename__ = "power"  # type: ignore

    __table_args__ = (
        UniqueConstraint("cups", "datetime", name="uq_power_cups_datetime"),
        {"extend_existing": True},
    )

    id: int | None = Field(default=None, primary_key=True)
    cups: str = Field(foreign_key="supply.cups", index=True)
    datetime: dt = Field(index=True)

    data: Power = Field(sa_column=Column(PydanticJSON(Power)))

    version: int = Field(default=1)
    created_at: dt = Field(default_factory=dt.now, nullable=False)
    updated_at: dt = Field(
        default_factory=dt.now, nullable=False, sa_column_kwargs={"onupdate": dt.now}
    )


class StatisticsModel(SQLModel, table=True):

    __tablename__ = "statistics"  # type: ignore

    __table_args__ = (
        UniqueConstraint(
            "cups",
            "datetime",
            "type",
            name="uq_statistics_datetime_type",
        ),
        {"extend_existing": True},
    )

    id: int | None = Field(default=None, primary_key=True)
    cups: str = Field(foreign_key="supply.cups", index=True)
    datetime: dt = Field(index=True)
    type: typing.Literal["day", "month"] = Field(index=True, sa_type=AutoString)
    complete: bool = Field(False)
    data: Statistics = Field(sa_column=Column(PydanticJSON(Statistics)))

    version: int = Field(default=1)
    created_at: dt = Field(default_factory=dt.now, nullable=False)
    updated_at: dt = Field(
        default_factory=dt.now, nullable=False, sa_column_kwargs={"onupdate": dt.now}
    )


class PVPCModel(SQLModel, table=True):

    __tablename__ = "pvpc"  # type: ignore

    id: int | None = Field(default=None, primary_key=True)
    datetime: dt = Field(index=True, unique=True)
    data: EnergyPrice = Field(sa_column=Column(PydanticJSON(EnergyPrice)))

    version: int = Field(default=1)
    created_at: dt = Field(default_factory=dt.now, nullable=False)
    updated_at: dt = Field(
        default_factory=dt.now, nullable=False, sa_column_kwargs={"onupdate": dt.now}
    )


class BillModel(SQLModel, table=True):

    __tablename__ = "bill"  # type: ignore

    __table_args__ = (
        UniqueConstraint(
            "cups",
            "datetime",
            "type",
            name="uq_bill_datetime_type",
        ),
        {"extend_existing": True},
    )

    id: int | None = Field(default=None, primary_key=True)
    cups: str = Field(foreign_key="supply.cups", index=True)
    datetime: dt = Field(index=True)
    type: typing.Literal["hour", "day", "month"] = Field(index=True, sa_type=AutoString)
    complete: bool = Field(False)
    confhash: str
    data: Bill = Field(sa_column=Column(PydanticJSON(Bill)))

    version: int = Field(default=1)
    created_at: dt = Field(default_factory=dt.now, nullable=False)
    updated_at: dt = Field(
        default_factory=dt.now, nullable=False, sa_column_kwargs={"onupdate": dt.now}
    )
