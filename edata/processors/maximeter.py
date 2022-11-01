"""Maximeter data processors"""

import logging
from datetime import datetime
from typing import TypedDict

import pandas as pd
from dateparser import parse

from .base import Processor

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MaximeterStats(TypedDict):
    """A dict holding MaximeterProcessor stats"""

    value_max_kW: float
    date_max: datetime
    value_mean_kW: float
    value_tile90_kW: float


class MaximeterOutput(TypedDict):
    """A dict holding MaximeterProcessor output property"""

    stats: MaximeterStats


class MaximeterProcessor(Processor):
    """A processor for Maximeter data"""

    def do_process(self):
        self._output = {"stats": {}}
        self._df = pd.DataFrame(self._input)
        self._df.round(2)
        if all(k in self._df for k in ("datetime", "value_kW")):
            idx = self._df["value_kW"].argmax()
            self._output["stats"] = MaximeterOutput(
                value_max_kW=round(self._df["value_kW"][idx], 2),
                date_max=parse(str(self._df["datetime"][idx])),
                value_mean_kW=self._df["value_kW"].mean().round(2),
                value_tile90_kW=self._df["value_kW"].quantile(0.9).round(2),
            )
        else:
            _LOGGER.warning("Wrong data structure")
            return False
