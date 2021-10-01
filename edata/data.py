"""Data structures definition"""

import datetime
from collections.abc import Iterable
from typing_extensions import TypedDict

# DRAFT

class Supply (TypedDict):
    """Supplies data class"""
    cups: str
    start: datetime.datetime
    end: datetime.datetime
    address: str
    postal_code: str
    province: str
    municipality: str
    distributor: str
    pointType: str
    distributorCode: str

class Contract (TypedDict):
    """Contracts data class."""
    cups: str
    start: datetime.datetime
    end: datetime.datetime
    marketer: str
    distributorCode: int
    power_p1: float
    power_p2: float
    power_p3: float
    power_p4: float
    power_p5: float
    power_p6: float

class Consumption (TypedDict):
    """Consumption data class."""
    start: datetime.datetime
    end: datetime.datetime
    delta_h: float
    value_kwh: float
    real: bool

class MaxPower (TypedDict):
    """MaxPower data class."""
    start: datetime.datetime
    end: datetime.datetime
    value_kw: float

class EnergyCost (TypedDict):
    """EnergyCost data class."""
    start: datetime.datetime
    end: datetime.datetime
    value_eur: float

Supplies = Iterable [Supply]
Contracts = Iterable [Contract]
Consumptions = Iterable [Consumption]
Maximeter = Iterable [MaxPower]
Prices = Iterable [EnergyCost]