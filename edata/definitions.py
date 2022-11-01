"""Definitions for data structures"""

import datetime as dt
import typing
from typing import TypedDict, _TypedDictMeta


class SupplyData(TypedDict):
    """Data structure to represent a supply"""

    cups: str
    date_start: dt.datetime
    date_end: dt.datetime
    address: typing.Optional[str]
    postal_code: typing.Optional[str]
    province: typing.Optional[str]
    municipality: typing.Optional[str]
    distributor: typing.Optional[str]
    pointType: int
    distributorCode: str


class ContractData(TypedDict):
    """Data structure to represent a contract"""

    date_start: dt.datetime
    date_end: dt.datetime
    marketer: str
    distributorCode: str
    power_p1: typing.Optional[float]
    power_p2: typing.Optional[float]


class ConsumptionData(TypedDict):
    """Data structure to represent a consumption"""

    datetime: dt.datetime
    delta_h: float
    value_kWh: float
    real: bool


class MaxPowerData(TypedDict):
    """Data structure to represent a MaxPower"""

    datetime: dt.datetime
    value_kW: float


class PricingData(TypedDict):
    """Data structure to represent pricing data"""

    datetime: dt.datetime
    value_eur_kWh: float
    delta_h: int


class PricingRules(TypedDict):
    """Data structure to represent custom pricing rules"""

    p1_kw_year_eur: float
    p2_kw_year_eur: float
    p1_kwh_eur: float
    p2_kwh_eur: float
    p3_kwh_eur: float
    meter_month_eur: float
    market_kw_year_eur: float
    electricity_tax: float
    iva_tax: float


def check_integrity(item: dict, definition: _TypedDictMeta):
    """Checks if an item follows a given definition"""
    if all(k in item for k in definition.__required_keys__):
        return True
    return False
