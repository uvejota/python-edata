"""Models for billing-related data"""

from datetime import datetime
from pydantic import BaseModel


class EnergyPrice(BaseModel):
    """Data structure to represent pricing data."""

    datetime: datetime
    value_eur_kWh: float
    delta_h: float
