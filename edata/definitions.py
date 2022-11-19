"""Definitions for data structures"""

import datetime as dt
import typing
from collections.abc import Iterable
from typing import TypedDict, _TypedDictMeta

ATTRIBUTES = {
    "cups": None,
    "contract_p1_kW": "kW",
    "contract_p2_kW": "kW",
    "yesterday_kWh": "kWh",
    "yesterday_hours": "h",
    "yesterday_p1_kWh": "kWh",
    "yesterday_p2_kWh": "kWh",
    "yesterday_p3_kWh": "kWh",
    "last_registered_date": None,
    "last_registered_day_kWh": "kWh",
    "last_registered_day_hours": "h",
    "last_registered_day_p1_kWh": "kWh",
    "last_registered_day_p2_kWh": "kWh",
    "last_registered_day_p3_kWh": "kWh",
    "month_kWh": "kWh",
    "month_daily_kWh": "kWh",
    "month_days": "d",
    "month_p1_kWh": "kWh",
    "month_p2_kWh": "kWh",
    "month_p3_kWh": "kWh",
    "month_€": "€",
    "last_month_kWh": "kWh",
    "last_month_daily_kWh": "kWh",
    "last_month_days": "d",
    "last_month_p1_kWh": "kWh",
    "last_month_p2_kWh": "kWh",
    "last_month_p3_kWh": "kWh",
    "last_month_€": "€",
    "max_power_kW": "kW",
    "max_power_date": None,
    "max_power_mean_kW": "kW",
    "max_power_90perc_kW": "kW",
}


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
    p1_kwh_eur: typing.Optional[float]
    p2_kwh_eur: typing.Optional[float]
    p3_kwh_eur: typing.Optional[float]
    meter_month_eur: float
    market_kw_year_eur: float
    electricity_tax: float
    iva_tax: float


DEFAULT_PVPC_RULES = PricingRules(
    p1_kw_year_eur=30.67266,
    p2_kw_year_eur=1.4243591,
    meter_month_eur=0.81,
    market_kw_year_eur=3.113,
    electricity_tax=1.0511300560,
    iva_tax=1.05,
    p1_kwh_eur=None,
    p2_kwh_eur=None,
    p3_kwh_eur=None,
)


class ConsumptionAggData(TypedDict):
    """A dict holding a Consumption item"""

    datetime: dt.datetime
    value_p1_kWh: float
    value_p2_kWh: float
    value_p3_kWh: float


class PricingAggData(TypedDict):
    """A dict holding a Billing item"""

    datetime: dt.datetime
    value_eur: float
    energy_term: float
    power_term: float
    others_term: float


class EdataData(TypedDict):
    """A Typed Dict to handle Edata Aggregated Data"""

    supplies: Iterable[SupplyData]
    contracts: Iterable[ContractData]
    consumptions: Iterable[ConsumptionData]
    maximeter: Iterable[MaxPowerData]
    pvpc: Iterable[PricingData]
    consumptions_daily_sum: Iterable[ConsumptionAggData]
    consumptions_monthly_sum: Iterable[ConsumptionAggData]
    cost_hourly_sum: Iterable[PricingAggData]
    cost_daily_sum: Iterable[PricingAggData]
    cost_monthly_sum: Iterable[PricingAggData]


def check_integrity(item: dict, definition: _TypedDictMeta):
    """Checks if an item follows a given definition"""
    if all(k in item for k in definition.__required_keys__):
        return True
    return False
