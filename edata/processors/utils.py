"""Generic utilities for processing data"""

import json
import logging
from copy import deepcopy
from datetime import date, datetime, timedelta
from json import JSONEncoder

import holidays

from ..definitions import (
    ConsumptionData,
    ContractData,
    MaxPowerData,
    PricingData,
    SupplyData,
    check_integrity,
)

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HOURS_P1 = [10, 11, 12, 13, 18, 19, 20, 21]
HOURS_P2 = [8, 9, 14, 15, 16, 17, 22, 23]
WEEKDAYS_P3 = [5, 6]


def is_empty(lst):
    """Checks if a list is empty"""
    return len(lst) == 0


def extract_dt_ranges(lst, dt_from, dt_to, gap_interval=timedelta(hours=1)):
    """Filters a list of dicts between two datetimes"""
    new_lst = []
    missing = []
    oldest_dt = None
    newest_dt = None
    last_dt = None
    if len(lst) > 0:
        sorted_lst = sorted(lst, key=lambda i: i["datetime"])
        last_dt = dt_from
        for i in sorted_lst:
            if dt_from <= i["datetime"] <= dt_to:
                if (i["datetime"] - last_dt) > gap_interval:
                    missing.append({"from": last_dt, "to": i["datetime"]})
                if i.get("value_kWh", 1) > 0:
                    if oldest_dt is None or i["datetime"] < oldest_dt:
                        oldest_dt = i["datetime"]
                    if newest_dt is None or i["datetime"] > newest_dt:
                        newest_dt = i["datetime"]
                if i["datetime"] != last_dt:  # remove duplicates
                    new_lst.append(i)
                    last_dt = i["datetime"] + timedelta(hours=i.get("delta_h", 0))
        if dt_to > last_dt:
            missing.append({"from": last_dt, "to": dt_to})
        _LOGGER.debug("found data from %s to %s", oldest_dt, newest_dt)
    else:
        missing.append({"from": dt_from, "to": dt_to})
    return new_lst, missing


def extend_by_key(old_lst, new_lst, key):
    """Extends a list of dicts by key"""
    lst = deepcopy(old_lst)
    temp_list = []
    for new_element in new_lst:
        for old_element in lst:
            if new_element[key] == old_element[key]:
                for i in old_element:
                    old_element[i] = new_element[i]
                break
        else:
            temp_list.append(new_element)
    lst.extend(temp_list)
    return lst


def get_by_key(lst, key, value):
    """Obtains an element of a list of dicts by key=value"""
    for i in lst:
        if i[key] == value:
            return i
    return None


def get_pvpc_tariff(a_datetime):
    """Evals the PVPC tariff for a given datetime"""
    hdays = holidays.country_holidays("ES")
    hour = a_datetime.hour
    weekday = a_datetime.weekday()
    if weekday in WEEKDAYS_P3 or a_datetime.date() in hdays:
        return "p3"
    elif hour in HOURS_P1:
        return "p1"
    elif hour in HOURS_P2:
        return "p2"
    else:
        return "p3"


def serialize_dict(data: dict) -> dict:
    """Serialize dicts as json"""

    class DateTimeEncoder(JSONEncoder):
        """Replace datetime objects with ISO strings"""

        def default(self, o):
            if isinstance(o, (date, datetime)):
                return o.isoformat()

    return json.loads(json.dumps(data, cls=DateTimeEncoder))


def deserialize_dict(serialized_dict: dict) -> dict:
    """Deserializes a json replacing ISOTIME strings into datetime"""

    def datetime_parser(json_dict):
        """Parse JSON while converting ISO strings into datetime objects"""
        for (key, value) in json_dict.items():
            if "date" in key:
                try:
                    json_dict[key] = datetime.fromisoformat(value)
                except Exception:
                    pass
        return json_dict

    data: dict = json.loads(json.dumps(serialized_dict), object_hook=datetime_parser)
    if data is not None and data != {}:
        for key in data:
            if key == "supplies":
                if not isinstance(data[key], list):
                    return None
                for i in data[key]:
                    if not check_integrity(i, SupplyData):
                        return None
            elif key == "contracts":
                if not isinstance(data[key], list):
                    return None
                for i in data[key]:
                    if not check_integrity(i, ContractData):
                        return None
            elif key == "consumptions":
                if not isinstance(data[key], list):
                    return None
                for i in data[key]:
                    if not check_integrity(i, ConsumptionData):
                        return None
            elif key == "maximeter":
                if not isinstance(data[key], list):
                    return None
                for i in data[key]:
                    if not check_integrity(i, MaxPowerData):
                        return None
            elif key == "pvpc":
                if not isinstance(data[key], list):
                    return None
                for i in data[key]:
                    if not check_integrity(i, PricingData):
                        return None
        return data
