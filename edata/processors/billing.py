"""Billing data processors."""

import logging
from datetime import datetime, timedelta
from typing import Optional, TypedDict
from jinja2 import Environment

import voluptuous

from ..definitions import (
    ConsumptionData,
    ConsumptionSchema,
    ContractData,
    ContractSchema,
    PricingAggData,
    PricingData,
    PricingRules,
    PricingRulesSchema,
    PricingSchema,
)
from ..processors import utils
from ..processors.base import Processor
import contextlib

_LOGGER = logging.getLogger(__name__)


class BillingOutput(TypedDict):
    """A dict holding BillingProcessor output property."""

    hourly: list[PricingAggData]
    daily: list[PricingAggData]
    monthly: list[PricingAggData]


class BillingInput(TypedDict):
    """A dict holding BillingProcessor input data."""

    contracts: list[ContractData]
    consumptions: list[ConsumptionData]
    prices: Optional[list[PricingData]]
    rules: PricingRules


class BillingProcessor(Processor):
    """A billing processor for edata."""

    def do_process(self):
        """Main method for the BillingProcessor."""
        self._output = BillingOutput(hourly=[], daily=[], monthly=[])

        _schema = voluptuous.Schema(
            {
                voluptuous.Required("contracts"): [ContractSchema],
                voluptuous.Required("consumptions"): [ConsumptionSchema],
                voluptuous.Optional("prices", default=None): voluptuous.Union(
                    [voluptuous.Union(PricingSchema)], None
                ),
                voluptuous.Required("rules"): PricingRulesSchema,
            }
        )
        self._input = _schema(self._input)

        self._cycle_offset = self._input["rules"]["cycle_start_day"] - 1

        # joint data by datetime
        _data = {
            x["datetime"]: {
                "datetime": x["datetime"],
                "kwh": x["value_kWh"],
                "surplus_kwh": x["surplus_kWh"] if x["surplus_kWh"] is not None else 0,
            }
            for x in self._input["consumptions"]
        }

        for contract in self._input["contracts"]:
            start = contract["date_start"]
            end = contract["date_end"]
            finish = False
            while not finish:
                if start in _data:
                    _data[start]["p1_kw"] = contract["power_p1"]
                    _data[start]["p2_kw"] = contract["power_p2"]
                start = start + timedelta(hours=1)
                finish = not (end > start)

        if self._input["prices"]:
            for x in self._input["prices"]:
                start = x["datetime"]
                if start in _data:
                    _data[start]["kwh_eur"] = x["value_eur_kWh"]

        env = Environment()
        energy_expr = env.compile_expression(
            f'({self._input["rules"]["energy_formula"]})|float|round(3)'
        )
        power_expr = env.compile_expression(
            f'({self._input["rules"]["power_formula"]})|float|round(3)'
        )
        others_expr = env.compile_expression(
            f'({self._input["rules"]["others_formula"]})|float|round(3)'
        )
        surplus_expr = env.compile_expression(
            f'({self._input["rules"]["surplus_formula"]})|float|round(3)'
        )

        _data = sorted([_data[x] for x in _data], key=lambda x: x["datetime"])
        hourly = []
        for x in _data:
            x.update(self._input["rules"])
            tariff = utils.get_pvpc_tariff(x["datetime"])
            if "kwh_eur" not in x:
                if tariff == "p1":
                    x["kwh_eur"] = x["p1_kwh_eur"]
                elif tariff == "p2":
                    x["kwh_eur"] = x["p2_kwh_eur"]
                elif tariff == "p3":
                    x["kwh_eur"] = x["p3_kwh_eur"]

                if x["kwh_eur"] is None:
                    continue

            if tariff == "p1":
                x["surplus_kwh_eur"] = x["surplus_p1_kwh_eur"]
            elif tariff == "p2":
                x["surplus_kwh_eur"] = x["surplus_p2_kwh_eur"]
            elif tariff == "p3":
                x["surplus_kwh_eur"] = x["surplus_p3_kwh_eur"]

            _energy_term = 0
            _power_term = 0
            _others_term = 0
            _surplus_term = 0

            with contextlib.suppress(Exception):
                _energy_term = round(energy_expr(**x), 3)
                _power_term = round(power_expr(**x), 3)
                _others_term = round(others_expr(**x), 3)
                _surplus_term = round(surplus_expr(**x), 3)

            new_item = PricingAggData(
                datetime=x["datetime"],
                energy_term=_energy_term,
                power_term=_power_term,
                others_term=_others_term,
                surplus_term=_surplus_term,
                value_eur=0,
                delta_h=1,
            )

            new_item["value_eur"] = round(
                new_item["energy_term"]
                + new_item["power_term"]
                + new_item["others_term"]
                - new_item["surplus_term"],
                3,
            )

            hourly.append(new_item)

        self._output["hourly"] = hourly

        last_day_dt = None
        last_month_dt = None
        for hour in hourly:
            curr_hour_dt: datetime = hour["datetime"]
            curr_day_dt = curr_hour_dt.replace(hour=0, minute=0, second=0)
            curr_month_dt = (curr_day_dt - timedelta(days=self._cycle_offset)).replace(
                day=1
            )

            if last_day_dt is None or curr_day_dt != last_day_dt:
                self._output["daily"].append(
                    PricingAggData(
                        datetime=curr_day_dt,
                        energy_term=hour["energy_term"],
                        power_term=hour["power_term"],
                        others_term=hour["others_term"],
                        surplus_term=hour["surplus_term"],
                        value_eur=hour["value_eur"],
                        delta_h=hour["delta_h"],
                    )
                )
            else:
                self._output["daily"][-1]["energy_term"] += hour["energy_term"]
                self._output["daily"][-1]["power_term"] += hour["power_term"]
                self._output["daily"][-1]["others_term"] += hour["others_term"]
                self._output["daily"][-1]["surplus_term"] += hour["surplus_term"]
                self._output["daily"][-1]["value_eur"] += hour["value_eur"]
                self._output["daily"][-1]["delta_h"] += hour["delta_h"]

            if last_month_dt is None or curr_month_dt != last_month_dt:
                self._output["monthly"].append(
                    PricingAggData(
                        datetime=curr_month_dt,
                        energy_term=hour["energy_term"],
                        power_term=hour["power_term"],
                        others_term=hour["others_term"],
                        surplus_term=hour["surplus_term"],
                        value_eur=hour["value_eur"],
                        delta_h=hour["delta_h"],
                    )
                )
            else:
                self._output["monthly"][-1]["energy_term"] += hour["energy_term"]
                self._output["monthly"][-1]["power_term"] += hour["power_term"]
                self._output["monthly"][-1]["others_term"] += hour["others_term"]
                self._output["monthly"][-1]["surplus_term"] += hour["surplus_term"]
                self._output["monthly"][-1]["value_eur"] += hour["value_eur"]
                self._output["monthly"][-1]["delta_h"] += hour["delta_h"]

            last_day_dt = curr_day_dt
            last_month_dt = curr_month_dt

        for item in self._output:
            for cost in self._output[item]:
                cost["energy_term"] = round(cost["energy_term"], 3)
                cost["power_term"] = round(cost["power_term"], 3)
                cost["others_term"] = round(cost["others_term"], 3)
                cost["surplus_term"] = round(cost["surplus_term"], 3)
                cost["value_eur"] = round(cost["value_eur"], 3)
