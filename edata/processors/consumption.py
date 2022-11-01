"""Consumption data processors"""

import logging
from datetime import datetime

import pandas as pd

from . import utils as DataUtils
from .base import Processor

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HOURS_P1 = [10, 11, 12, 13, 18, 19, 20, 21]
HOURS_P2 = [8, 9, 14, 15, 16, 17, 22, 23]
WEEKDAYS_P3 = [5, 6]


class ConsumptionProcessor(Processor):
    """A consumptions processor"""

    _LABEL = "ConsumptionProcessor"

    def do_process(self):
        self._output = {"hourly": [], "daily": [], "monthly": []}
        self._df = pd.DataFrame(self._input)
        if all(k in self._df for k in ("datetime", "value_kWh")):
            self._df["datetime"] = pd.to_datetime(self._df["datetime"])
            self._df["weekday"] = self._df["datetime"].dt.day_name()
            self._df["px"] = self._df["datetime"].apply(DataUtils.get_pvpc_tariff)
            self._output["hourly"] = self._df.to_dict("records")
            for opt in [
                {
                    "date_format": "%Y-%m-01T00:00:00",
                    "period": "M",
                    "dictkey": "monthly",
                },
                {"date_format": "%Y-%m-%dT00:00:00", "period": "D", "dictkey": "daily"},
            ]:
                _t = self._df.copy()
                for p in ["p1", "p2", "p3"]:
                    _t["value_" + p + "_kWh"] = _t.loc[_t["px"] == p, "value_kWh"]
                _t.drop(["real"], axis=1, inplace=True)
                _t = _t.groupby([_t.datetime.dt.to_period(opt["period"])]).sum()
                _t.reset_index(inplace=True)
                _t["datetime"] = _t["datetime"].dt.strftime(opt["date_format"])
                _t = _t.round(2)
                self._output[opt["dictkey"]] = DataUtils.deserialize_dict(
                    _t.to_dict("records")
                )
            self._ready = True
        else:
            _LOGGER.warning("Wrong data structure")
            return False
