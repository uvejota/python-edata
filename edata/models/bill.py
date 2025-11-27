"""Models for billing-related data"""

from datetime import datetime

from pydantic import BaseModel, Field


class EnergyPrice(BaseModel):
    """Data structure to represent pricing data."""

    datetime: datetime
    value_eur_kWh: float
    delta_h: float


class Bill(BaseModel):
    """Data structure to represent a bill during a period."""

    datetime: datetime
    delta_h: float
    value_eur: float = Field(default=0.0)
    energy_term: float = Field(default=0.0)
    power_term: float = Field(default=0.0)
    others_term: float = Field(default=0.0)
    surplus_term: float = Field(default=0.0)


class BillingRules(BaseModel):
    """Data structure to represent a generic billing rule."""

    p1_kw_year_eur: float
    p2_kw_year_eur: float
    p1_kwh_eur: float
    p2_kwh_eur: float
    p3_kwh_eur: float
    surplus_p1_kwh_eur: float | None = Field(default=None)
    surplus_p2_kwh_eur: float | None = Field(default=None)
    surplus_p3_kwh_eur: float | None = Field(default=None)
    meter_month_eur: float = Field(default=0.81)
    market_kw_year_eur: float = Field(default=3.113)
    electricity_tax: float = Field(default=1.0511300560)
    iva_tax: float = Field(default=1.21)
    energy_formula: str = Field(default="electricity_tax * iva_tax * kwh_eur * kwh")
    power_formula: str = Field(
        default="electricity_tax * iva_tax * (p1_kw * (p1_kw_year_eur + market_kw_year_eur) + p2_kw * p2_kw_year_eur) / 365 / 24"
    )
    others_formula: str = Field(default="iva_tax * meter_month_eur / 30 / 24")
    surplus_formula: str | None = Field(default=None)
    cycle_start_day: int = Field(default=1)


class PVPCBillingRules(BillingRules):
    """Data structure to represent a PVPC billing rule."""

    p1_kw_year_eur: float = Field(default=30.67266)
    p2_kw_year_eur: float = Field(default=1.4243591)
    p1_kwh_eur: None = Field(default=None)
    p2_kwh_eur: None = Field(default=None)
    p3_kwh_eur: None = Field(default=None)
    surplus_p1_kwh_eur: None = Field(default=None)
    surplus_p2_kwh_eur: None = Field(default=None)
    surplus_p3_kwh_eur: None = Field(default=None)
    meter_month_eur: float = Field(default=0.81)
    market_kw_year_eur: float = Field(default=3.113)
    electricity_tax: float = Field(default=1.0511300560)
    iva_tax: float = Field(default=1.21)
    energy_formula: str = Field(default="electricity_tax * iva_tax * kwh_eur * kwh")
    power_formula: str = Field(
        default="electricity_tax * iva_tax * (p1_kw * (p1_kw_year_eur + market_kw_year_eur) + p2_kw * p2_kw_year_eur) / 365 / 24"
    )
    others_formula: str = Field(default="iva_tax * meter_month_eur / 30 / 24")
    surplus_formula: str | None = Field(default=None)
    cycle_start_day: int = Field(default=1)
