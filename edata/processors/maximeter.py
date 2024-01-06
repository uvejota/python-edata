"""Maximeter data processors."""

import logging
from datetime import datetime
from typing import TypedDict

from dateparser import parse
import voluptuous

from edata.definitions import MaxPowerSchema
from edata.processors import utils

from .base import Processor

_LOGGER = logging.getLogger(__name__)


class MaximeterStats(TypedDict):
    """A dict holding MaximeterProcessor stats."""

    value_max_kW: float
    date_max: datetime
    value_mean_kW: float
    value_tile90_kW: float


class MaximeterOutput(TypedDict):
    """A dict holding MaximeterProcessor output property."""

    stats: MaximeterStats


class MaximeterProcessor(Processor):
    """A processor for Maximeter data."""

    def do_process(self):
        """Calculate maximeter stats."""

        self._output = MaximeterOutput(stats={})

        _schema = voluptuous.Schema([MaxPowerSchema])
        self._input = _schema(self._input)

        _values = [x["value_kW"] for x in self._input]

        _max_kW = max(_values)
        _dt_max_kW = parse(str(self._input[_values.index(_max_kW)]["datetime"]))
        _mean_kW = sum(_values) / len(_values)
        _tile90_kW = utils.percentile(_values, 0.9)

        self._output["stats"] = MaximeterOutput(
            value_max_kW=round(_max_kW, 2),
            date_max=_dt_max_kW,
            value_mean_kW=round(_mean_kW, 2),
            value_tile90_kW=round(_tile90_kW, 2),
        )
