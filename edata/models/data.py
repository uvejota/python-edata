"""Models for telemetry data"""

from datetime import datetime

from pydantic import BaseModel, Field


class Energy(BaseModel):
    """Represent energy consumption and/or surplus measurements."""

    datetime: datetime
    delta_h: float
    consumption_kwh: float
    surplus_kwh: float = Field(0)
    real: bool


class Power(BaseModel):
    """Represent power measurements."""

    datetime: datetime
    value_kW: float


class Statistics(BaseModel):
    """Represent aggregated energy/surplus data."""

    datetime: datetime
    delta_h: float = Field(0)
    consumption_kwh: float = Field(0)
    consumption_by_tariff: list[float] = Field(default_factory=lambda: [0.0, 0.0, 0.0])
    surplus_kwh: float = Field(0)
    surplus_by_tariff: list[float] = Field(default_factory=lambda: [0.0, 0.0, 0.0])

    @property
    def consumption_p1_kwh(self) -> float:
        return self.consumption_by_tariff[0]

    @property
    def consumption_p2_kwh(self) -> float:
        return self.consumption_by_tariff[1]

    @property
    def consumption_p3_kwh(self) -> float:
        return self.consumption_by_tariff[2]

    @property
    def surplus_p1_kwh(self) -> float:
        return self.surplus_by_tariff[0]

    @property
    def surplus_p2_kwh(self) -> float:
        return self.surplus_by_tariff[1]

    @property
    def surplus_p3_kwh(self) -> float:
        return self.surplus_by_tariff[2]
