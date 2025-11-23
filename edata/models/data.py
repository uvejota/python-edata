"""Models for telemetry data"""

from datetime import datetime
from pydantic import BaseModel


class Energy(BaseModel):
    """Data structure to represent energy consumption and/or surplus during a period."""

    datetime: datetime
    delta_h: float
    value_kWh: float
    surplus_kWh: float
    real: bool


class Power(BaseModel):
    """Data structure to represent power measurements."""

    datetime: datetime
    value_kW: float
