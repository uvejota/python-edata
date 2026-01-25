"""Models for contractual data"""

from datetime import datetime

from pydantic import BaseModel


class Supply(BaseModel):
    """Data model of a Supply."""

    cups: str
    date_start: datetime
    date_end: datetime
    address: str | None
    postal_code: str | None
    province: str | None
    municipality: str | None
    distributor: str | None
    point_type: int
    distributor_code: str


class Contract(BaseModel):
    """Data model of a Contract."""

    date_start: datetime
    date_end: datetime
    marketer: str
    distributor_code: str
    power: list[float]

    @property
    def power_p1(self) -> float | None:
        """Return power P1."""
        return self.power[0] if len(self.power) > 0 else None

    @property
    def power_p2(self) -> float | None:
        """Return power P2."""
        return self.power[1] if len(self.power) > 1 else None
