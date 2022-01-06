"""Data structures definition"""

from __future__ import annotations

from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

from typing_extensions import TypedDict


# Base Exception
class EdataError(Exception):
    """Edata base exception"""

    def __init__(self, source, message):
        self.source = source
        self.message = message


# DATA TYPES


class Supply(TypedDict):
    """Supplies data class"""

    cups: str
    start: datetime
    end: datetime
    address: str | None
    postal_code: str | None
    province: str | None
    municipality: str | None
    distributor: str | None
    point_type: str
    distributor_code: str


class Contract(TypedDict):
    """Contracts data class."""

    cups: str
    start: datetime
    end: datetime
    marketer: str
    distributor_code: int
    power_p1: float | None
    power_p2: float | None
    power_p3: float | None
    power_p4: float | None
    power_p5: float | None
    power_p6: float | None


class Consumption(TypedDict):
    """Consumption data class."""

    start: datetime
    end: datetime
    value_kwh: float
    real: bool


class MaxPower(TypedDict):
    """MaxPower data class."""

    start: datetime
    end: datetime
    time: datetime
    value_kw: float


class Cost(TypedDict):
    """Cost data class."""

    start: datetime
    end: datetime
    value_eur: float


class DatadisData(TypedDict):
    """Datadis data class"""

    supplies: Iterable[Supply]
    contracts: Iterable[Contract]
    consumptions: Iterable[Consumption]
    maximeter: Iterable[MaxPower]


class EsiosData(TypedDict):
    """Esios data class"""

    energy_costs: Iterable[Cost]


class EdataData(TypedDict):
    """Edata data class"""

    supplies: Iterable[Supply]
    contracts: Iterable[Contract]
    consumptions: dict[str, Iterable[Consumption]]
    maximeter: dict[str, Iterable[MaxPower] | dict[str, float]]
    costs: dict[str, Iterable[Cost]]


class TariffData(TypedDict):
    """Tariff data class also containing variable energy costs"""

    hours: Iterable[int]
    weekdays: Iterable[int]
    cost_kwh: float | None  # assuming pvpc if None


class FixedCostsData(TypedDict):
    """A price data class for contractual fixed costs"""

    cost_p1_kw: float
    cost_p2_kw: float
    market_p1_kw: float
    others_day: float
    electricity_tax: float
    iva_tax: float


class CostsData(TypedDict):
    """A class to hold a pricing rule"""

    start: datetime
    end: datetime | None
    tariffs: dict[TariffData]
    fixed_costs_daily: FixedCostsData


# DATA HANDLERS.


def add_or_update(
    base_items: Iterable[dict], new_items: Iterable[dict], key
) -> Iterable[dict]:
    """Add or update dict in list"""
    new_list = deepcopy(base_items)
    nn = []
    for n in new_items:
        for o in new_list:
            if n[key] == o[key]:
                for i in o:
                    o[i] = n[i]
                break
        else:
            nn.append(n)
    new_list.extend(nn)
    return new_list


def get_by_key(lst: Iterable[dict], key: str, value: Any) -> dict:
    for i in lst:
        if i[key] == value:
            return i
    else:
        return {}


def filter_by_key_in_range(
    lst: Iterable[dict], key: str, dt_from: datetime, dt_to: datetime
) -> Iterable[dict]:
    """Filter a list of dicts if provided key is out of a given range"""
    return [x for x in lst if dt_from <= x[key] <= dt_to]


def sort_by_key(lst: Iterable[dict], key: str) -> Iterable[dict]:
    """Sort order of lists of dicts based of a key"""
    return sorted(lst, key=lambda i: i[key])


def remove_duplicates_by_key(lst: Iterable[dict], key: str) -> Iterable[dict]:
    new_lst = []
    [new_lst.append(x) for x in lst if x[key] not in [y[key] for y in new_lst]]
    return new_lst


def find_gaps(
    lst: Iterable[dict],
    dt_from: datetime,
    dt_to: datetime,
    gap_interval: timedelta = timedelta(hours=1),
    dt_key: str = "start",
) -> Iterable[dict]:
    """Filters, sorts, remove duplicates and find missing gaps"""

    # filter range
    _lst = filter_by_key_in_range(lst, dt_key, dt_from, dt_to)

    # sort by key
    _lst = sort_by_key(_lst, dt_key)

    # remove duplicates
    _lst = remove_duplicates_by_key(_lst, dt_key)

    # find gaps
    gaps = []
    _last = dt_from
    for i in _lst:
        if (i[dt_key] - _last) > gap_interval:
            if len(gaps) > 0 and gaps[-1]["to"] == _last:
                gaps[-1]["to"] = i[dt_key]
            else:
                gaps.append({"from": _last, "to": i[dt_key]})
        _last = i[dt_key]
    if (dt_to - _last) > gap_interval:
        gaps.append({"from": _last, "to": dt_to})

    return _lst, gaps
