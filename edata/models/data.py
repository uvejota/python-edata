"""Models for telemetry data"""

from datetime import datetime

from pydantic import BaseModel, Field


class Energy(BaseModel):
    """Represent energy consumption and/or surplus measurements."""

    datetime: datetime
    delta_h: float
    value_kWh: float
    surplus_kWh: float = Field(0)
    real: bool


class Power(BaseModel):
    """Represent power measurements."""

    datetime: datetime
    value_kW: float


class Statistics(BaseModel):
    """Represent aggregated energy/surplus data."""

    datetime: datetime
    delta_h: float = Field(0)
    value_kWh: float = Field(0)
    value_p1_kWh: float = Field(0)
    value_p2_kWh: float = Field(0)
    value_p3_kWh: float = Field(0)
    surplus_kWh: float = Field(0)
    surplus_p1_kWh: float = Field(0)
    surplus_p2_kWh: float = Field(0)
    surplus_p3_kWh: float = Field(0)
