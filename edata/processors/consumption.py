"""Consumption data processors"""

import logging
from collections.abc import Iterable
from typing import TypedDict

import pandas as pd

from ..definitions import ConsumptionAggData, ConsumptionData, check_integrity
from . import utils
from .base import Processor

_LOGGER = logging.getLogger(__name__)


class ConsumptionOutput(TypedDict):
    """A dict holding ConsumptionProcessor output property"""

    hourly: Iterable[ConsumptionAggData]
    daily: Iterable[ConsumptionAggData]
    monthly: Iterable[ConsumptionAggData]


class ConsumptionProcessor(Processor):
    """A consumptions processor"""

    def do_process(self):
        self._output = ConsumptionOutput(hourly=[], daily=[], monthly=[])
        self._df = pd.DataFrame(self._input)
        if check_integrity(self._df, ConsumptionData):
            self._df["datetime"] = pd.to_datetime(self._df["datetime"])
            self._df["weekday"] = self._df["datetime"].dt.day_name()
            self._df["px"] = self._df["datetime"].apply(utils.get_pvpc_tariff)
            _t = self._df.copy()
            _t["datetime"] = _t["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")
            self._output["hourly"] = utils.deserialize_dict(
                _t.round(3).to_dict("records")
            )
            for opt in (
                {
                    "date_format": "%Y-%m-01T00:00:00",
                    "period": "M",
                    "dictkey": "monthly",
                },
                {"date_format": "%Y-%m-%dT00:00:00", "period": "D", "dictkey": "daily"},
            ):
                _t = self._df.copy()
                for tariff in ("p1", "p2", "p3"):
                    _t["value_" + tariff + "_kWh"] = _t.loc[
                        _t["px"] == tariff, "value_kWh"
                    ]
                _t.drop(["real"], axis=1, inplace=True)
                _t = _t.groupby([_t.datetime.dt.to_period(opt["period"])]).sum()
                _t.reset_index(inplace=True)
                _t["datetime"] = _t["datetime"].dt.strftime(opt["date_format"])
                _t = _t.round(2)
                self._output[opt["dictkey"]] = utils.deserialize_dict(
                    _t.to_dict("records")
                )
            self._ready = True
        else:
            _LOGGER.warning("Wrong data structure")
            return False
