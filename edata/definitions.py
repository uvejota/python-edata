"""Definitions for data structures"""

import datetime as dt
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
    address: str | None
    postal_code: str | None
    province: str | None
    municipality: str | None
    distributor: str | None
    pointType: int
    distributorCode: str


class ContractData(TypedDict):
    """Data structure to represent a contract"""

    date_start: dt.datetime
    date_end: dt.datetime
    marketer: str
    distributorCode: str
    power_p1: float | None
    power_p2: float | None


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
    p1_kwh_eur: float | None
    p2_kwh_eur: float | None
    p3_kwh_eur: float | None
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

    supplies: list[SupplyData]
    contracts: list[ContractData]
    consumptions: list[ConsumptionData]
    maximeter: list[MaxPowerData]
    pvpc: list[PricingData]
    consumptions_daily_sum: list[ConsumptionAggData]
    consumptions_monthly_sum: list[ConsumptionAggData]
    cost_hourly_sum: list[PricingAggData]
    cost_daily_sum: list[PricingAggData]
    cost_monthly_sum: list[PricingAggData]


def check_integrity(item: Iterable, definition: _TypedDictMeta):
    """Checks if an item follows a given definition"""
    if all(k in item for k in definition.__required_keys__):
        return True
    return False
