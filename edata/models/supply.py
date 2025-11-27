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
    pointType: int
    distributorCode: str


class Contract(BaseModel):
    """Data model of a Contract."""

    date_start: datetime
    date_end: datetime
    marketer: str
    distributorCode: str
    power_p1: float | None
    power_p2: float | None
