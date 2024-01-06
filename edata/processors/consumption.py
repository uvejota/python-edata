"""Consumption data processors."""

import logging
from collections.abc import Iterable
from typing import TypedDict
from datetime import datetime

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

        _schema = voluptuous.Schema([ConsumptionSchema])
        self._input = _schema(self._input)

        for consumption in self._input:
            curr_hour_dt: datetime = consumption["datetime"]
            curr_day_dt = curr_hour_dt.replace(hour=0, minute=0, second=0)
            curr_month_dt = curr_day_dt.replace(day=1)

            tariff = utils.get_pvpc_tariff(curr_hour_dt)
            kwh = consumption["value_kWh"]
            delta_h = consumption["delta_h"]

            kwh_p1 = 0
            kwh_p2 = 0
            kwh_p3 = 0

            match tariff:
                case "p1":
                    kwh_p1 = kwh
                case "p2":
                    kwh_p2 = kwh
                case "p3":
                    kwh_p3 = kwh

            if last_day_dt is None or curr_day_dt != last_day_dt:
                self._output["daily"].append(
                    ConsumptionAggData(
                        datetime=curr_day_dt,
                        value_kWh=kwh,
                        delta_h=delta_h,
                        value_p1_kWh=kwh_p1,
                        value_p2_kWh=kwh_p2,
                        value_p3_kWh=kwh_p3,
                    )
                )
            else:
                self._output["daily"][-1]["value_kWh"] += kwh
                self._output["daily"][-1]["value_p1_kWh"] += kwh_p1
                self._output["daily"][-1]["value_p2_kWh"] += kwh_p2
                self._output["daily"][-1]["value_p3_kWh"] += kwh_p3
                self._output["daily"][-1]["delta_h"] += delta_h

            if last_month_dt is None or curr_month_dt != last_month_dt:
                self._output["monthly"].append(
                    ConsumptionAggData(
                        datetime=curr_month_dt,
                        value_kWh=kwh,
                        delta_h=delta_h,
                        value_p1_kWh=kwh_p1,
                        value_p2_kWh=kwh_p2,
                        value_p3_kWh=kwh_p3,
                    )
                )
            else:
                self._output["monthly"][-1]["value_kWh"] += kwh
                self._output["monthly"][-1]["value_p1_kWh"] += kwh_p1
                self._output["monthly"][-1]["value_p2_kWh"] += kwh_p2
                self._output["monthly"][-1]["value_p3_kWh"] += kwh_p3
                self._output["monthly"][-1]["delta_h"] += delta_h

            last_day_dt = curr_day_dt
            last_month_dt = curr_month_dt

        for item in self._output:
            for cons in self._output[item]:
                cons["value_kWh"] = round(cons["value_kWh"], 2)
                cons["value_p1_kWh"] = round(cons["value_p1_kWh"], 2)
                cons["value_p2_kWh"] = round(cons["value_p2_kWh"], 2)
                cons["value_p3_kWh"] = round(cons["value_p3_kWh"], 2)
