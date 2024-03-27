"""Consumption data processors."""

import logging
from collections.abc import Iterable
from typing import TypedDict
from datetime import datetime, timedelta

import voluptuous

from ..definitions import ConsumptionAggData, ConsumptionSchema
from . import utils
from .base import Processor

_LOGGER = logging.getLogger(__name__)


class ConsumptionOutput(TypedDict):
    """A dict holding ConsumptionProcessor output property."""

    daily: Iterable[ConsumptionAggData]
    monthly: Iterable[ConsumptionAggData]


class ConsumptionProcessor(Processor):
    """A consumptions processor."""

    def do_process(self):
        """Calculate daily and monthly consumption stats."""

        self._output = ConsumptionOutput(daily=[], monthly=[])

        last_day_dt = None
        last_month_dt = None

        _schema = voluptuous.Schema(
            {
                voluptuous.Required("consumptions"): [ConsumptionSchema],
                voluptuous.Optional("cycle_start_day", default=1): voluptuous.Range(
                    1, 30
                ),
            }
        )
        self._input = _schema(self._input)

        self._cycle_offset = self._input["cycle_start_day"] - 1

        for consumption in self._input["consumptions"]:
            curr_hour_dt: datetime = consumption["datetime"]
            curr_day_dt = curr_hour_dt.replace(hour=0, minute=0, second=0)
            curr_month_dt = (curr_day_dt - timedelta(days=self._cycle_offset)).replace(
                day=1
            )

            tariff = utils.get_pvpc_tariff(curr_hour_dt)
            kwh = consumption["value_kWh"]
            surplus_kwh = consumption["surplus_kWh"]
            delta_h = consumption["delta_h"]

            kwh_by_tariff = [0, 0, 0]
            surplus_kwh_by_tariff = [0, 0, 0]

            match tariff:
                case "p1":
                    kwh_by_tariff[0] = kwh
                    surplus_kwh_by_tariff[0] = surplus_kwh
                case "p2":
                    kwh_by_tariff[1] = kwh
                    surplus_kwh_by_tariff[1] = surplus_kwh
                case "p3":
                    kwh_by_tariff[2] = kwh
                    surplus_kwh_by_tariff[2] = surplus_kwh

            if last_day_dt is None or curr_day_dt != last_day_dt:
                self._output["daily"].append(
                    ConsumptionAggData(
                        datetime=curr_day_dt,
                        value_kWh=kwh,
                        delta_h=delta_h,
                        value_p1_kWh=kwh_by_tariff[0],
                        value_p2_kWh=kwh_by_tariff[1],
                        value_p3_kWh=kwh_by_tariff[2],
                        surplus_kWh=surplus_kwh,
                        surplus_p1_kWh=surplus_kwh_by_tariff[0],
                        surplus_p2_kWh=surplus_kwh_by_tariff[1],
                        surplus_p3_kWh=surplus_kwh_by_tariff[2],
                    )
                )
            else:
                self._output["daily"][-1]["value_kWh"] += kwh
                self._output["daily"][-1]["value_p1_kWh"] += kwh_by_tariff[0]
                self._output["daily"][-1]["value_p2_kWh"] += kwh_by_tariff[1]
                self._output["daily"][-1]["value_p3_kWh"] += kwh_by_tariff[2]
                self._output["daily"][-1]["surplus_kWh"] += surplus_kwh
                self._output["daily"][-1]["surplus_p1_kWh"] += surplus_kwh_by_tariff[0]
                self._output["daily"][-1]["surplus_p2_kWh"] += surplus_kwh_by_tariff[1]
                self._output["daily"][-1]["surplus_p3_kWh"] += surplus_kwh_by_tariff[2]
                self._output["daily"][-1]["delta_h"] += delta_h

            if last_month_dt is None or curr_month_dt != last_month_dt:
                self._output["monthly"].append(
                    ConsumptionAggData(
                        datetime=curr_month_dt,
                        value_kWh=kwh,
                        delta_h=delta_h,
                        value_p1_kWh=kwh_by_tariff[0],
                        value_p2_kWh=kwh_by_tariff[1],
                        value_p3_kWh=kwh_by_tariff[2],
                        surplus_kWh=surplus_kwh,
                        surplus_p1_kWh=surplus_kwh_by_tariff[0],
                        surplus_p2_kWh=surplus_kwh_by_tariff[1],
                        surplus_p3_kWh=surplus_kwh_by_tariff[2],
                    )
                )
            else:
                self._output["monthly"][-1]["value_kWh"] += kwh
                self._output["monthly"][-1]["value_p1_kWh"] += kwh_by_tariff[0]
                self._output["monthly"][-1]["value_p2_kWh"] += kwh_by_tariff[1]
                self._output["monthly"][-1]["value_p3_kWh"] += kwh_by_tariff[2]
                self._output["monthly"][-1]["surplus_kWh"] += surplus_kwh
                self._output["monthly"][-1]["surplus_p1_kWh"] += surplus_kwh_by_tariff[
                    0
                ]
                self._output["monthly"][-1]["surplus_p2_kWh"] += surplus_kwh_by_tariff[
                    1
                ]
                self._output["monthly"][-1]["surplus_p3_kWh"] += surplus_kwh_by_tariff[
                    2
                ]
                self._output["monthly"][-1]["delta_h"] += delta_h

            last_day_dt = curr_day_dt
            last_month_dt = curr_month_dt

        # Round to two decimals
        for item in self._output:
            for cons in self._output[item]:
                for key in cons:
                    if isinstance(cons[key], float):
                        cons[key] = round(cons[key], 2)
