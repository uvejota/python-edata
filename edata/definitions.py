"""Definitions for data structures."""

import voluptuous as vol
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
    """Data structure to represent a supply."""

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


SupplySchema = vol.Schema(
    {
        vol.Required("cups"): str,
        vol.Required("date_start"): dt.datetime,
        vol.Required("date_end"): dt.datetime,
        vol.Required("address"): vol.Union(str, None),
        vol.Required("postal_code"): vol.Union(str, None),
        vol.Required("province"): vol.Union(str, None),
        vol.Required("municipality"): vol.Union(str, None),
        vol.Required("distributor"): vol.Union(str, None),
        vol.Required("pointType"): int,
        vol.Required("distributorCode"): str,
    }
)


class ContractData(TypedDict):
    """Data structure to represent a contract."""

    date_start: dt.datetime
    date_end: dt.datetime
    marketer: str
    distributorCode: str
    power_p1: float | None
    power_p2: float | None


ContractSchema = vol.Schema(
    {
        vol.Required("date_start"): dt.datetime,
        vol.Required("date_end"): dt.datetime,
        vol.Required("marketer"): str,
        vol.Required("distributorCode"): str,
        vol.Required("power_p1"): vol.Union(float, None),
        vol.Required("power_p2"): vol.Union(float, None),
    }
)


class ConsumptionData(TypedDict):
    """Data structure to represent a consumption."""

    datetime: dt.datetime
    delta_h: float
    value_kWh: float
    surplus_kWh: float
    real: bool


ConsumptionSchema = vol.Schema(
    {
        vol.Required("datetime"): dt.datetime,
        vol.Required("delta_h"): vol.Coerce(float),
        vol.Required("value_kWh"): vol.Coerce(float),
        vol.Optional("surplus_kWh", default=0): vol.Coerce(float),
        vol.Required("real"): bool,
    }
)


class MaxPowerData(TypedDict):
    """Data structure to represent a MaxPower."""

    datetime: dt.datetime
    value_kW: float


MaxPowerSchema = vol.Schema(
    {
        vol.Required("datetime"): dt.datetime,
        vol.Required("value_kW"): vol.Coerce(float),
    }
)


class PricingData(TypedDict):
    """Data structure to represent pricing data."""

    datetime: dt.datetime
    value_eur_kWh: float
    delta_h: float


PricingSchema = vol.Schema(
    {
        vol.Required("datetime"): dt.datetime,
        vol.Required("value_eur_kWh"): vol.Coerce(float),
        vol.Required("delta_h"): vol.Coerce(float),
    }
)


class PricingRules(TypedDict):
    """Data structure to represent custom pricing rules."""

    p1_kw_year_eur: float
    p2_kw_year_eur: float
    p1_kwh_eur: float | None
    p2_kwh_eur: float | None
    p3_kwh_eur: float | None
    surplus_p1_kwh_eur: float | None
    surplus_p2_kwh_eur: float | None
    surplus_p3_kwh_eur: float | None
    meter_month_eur: float
    market_kw_year_eur: float
    electricity_tax: float
    iva_tax: float


PricingRulesSchema = vol.Schema(
    {
        vol.Required("p1_kw_year_eur"): vol.Coerce(float),
        vol.Required("p2_kw_year_eur"): vol.Coerce(float),
        vol.Required("p1_kwh_eur"): vol.Union(vol.Coerce(float), None),
        vol.Required("p2_kwh_eur"): vol.Union(vol.Coerce(float), None),
        vol.Required("p3_kwh_eur"): vol.Union(vol.Coerce(float), None),
        vol.Optional("surplus_p1_kwh_eur", default=0): vol.Union(
            vol.Coerce(float), None
        ),
        vol.Optional("surplus_p2_kwh_eur", default=0): vol.Union(
            vol.Coerce(float), None
        ),
        vol.Optional("surplus_p3_kwh_eur", default=0): vol.Union(
            vol.Coerce(float), None
        ),
        vol.Required("meter_month_eur"): vol.Coerce(float),
        vol.Required("market_kw_year_eur"): vol.Coerce(float),
        vol.Required("electricity_tax"): vol.Coerce(float),
        vol.Required("iva_tax"): vol.Coerce(float),
    }
)

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
    """A dict holding a Consumption item."""

    datetime: dt.datetime
    value_kWh: float
    value_p1_kWh: float
    value_p2_kWh: float
    value_p3_kWh: float
    delta_h: float


ConsumptionAggSchema = vol.Schema(
    {
        vol.Required("datetime"): dt.datetime,
        vol.Required("value_kWh"): vol.Coerce(float),
        vol.Required("value_p1_kWh"): vol.Coerce(float),
        vol.Required("value_p2_kWh"): vol.Coerce(float),
        vol.Required("value_p3_kWh"): vol.Coerce(float),
        vol.Required("delta_h"): vol.Coerce(float),
    }
)


class PricingAggData(TypedDict):
    """A dict holding a Billing item."""

    datetime: dt.datetime
    value_eur: float
    energy_term: float
    power_term: float
    others_term: float
    surplus_term: float
    delta_h: float


PricingAggSchema = vol.Schema(
    {
        vol.Required("datetime"): dt.datetime,
        vol.Required("value_eur"): vol.Coerce(float),
        vol.Required("energy_term"): vol.Coerce(float),
        vol.Required("power_term"): vol.Coerce(float),
        vol.Required("others_term"): vol.Coerce(float),
        vol.Optional("surplus_term", default=0): vol.Coerce(float),
        vol.Optional("delta_h", default=1): vol.Coerce(float),
    },
)


class EdataData(TypedDict):
    """A Typed Dict to handle Edata Aggregated Data."""

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


EdataSchema = vol.Schema(
    {
        vol.Required("supplies"): [SupplySchema],
        vol.Required("contracts"): [ContractSchema],
        vol.Required("consumptions"): [ConsumptionSchema],
        vol.Required("maximeter"): [MaxPowerSchema],
        vol.Optional("pvpc", default=[]): [PricingSchema],
        vol.Optional("consumptions_daily_sum", []): [ConsumptionAggSchema],
        vol.Optional("consumptions_monthly_sum", []): [ConsumptionAggSchema],
        vol.Optional("cost_hourly_sum", default=[]): [PricingAggSchema],
        vol.Optional("cost_daily_sum", default=[]): [PricingAggSchema],
        vol.Optional("cost_monthly_sum", default=[]): [PricingAggSchema],
    }
)


def check_integrity(item: Iterable, definition: _TypedDictMeta):
    """Check if an item follows a given definition."""
    if all(k in item for k in definition.__required_keys__):
        return True
    return True
