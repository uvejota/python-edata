import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime, timedelta

import holidays
import pandas as pd

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HOURS_P1 = [10, 11, 12, 13, 18, 19, 20, 21]
HOURS_P2 = [8, 9, 14, 15, 16, 17, 22, 23]
WEEKDAYS_P3 = [5, 6]


class DataUtils:
    """A collection of static methods to process datasets"""

    @staticmethod
    def is_empty(lst):
        return len(lst) == 0

    @staticmethod
    def extract_dt_ranges(lst, dt_from, dt_to, gap_interval=timedelta(hours=1)):
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
                        last_dt = i["datetime"]
            if dt_to > last_dt:
                missing.append({"from": last_dt, "to": dt_to})
            _LOGGER.debug(f"found data from {oldest_dt} to {newest_dt}")
        else:
            missing.append({"from": dt_from, "to": dt_to})
        return new_lst, missing

    @staticmethod
    def extend_by_key(old_lst, new_lst, key):
        lst = deepcopy(old_lst)
        nn = []
        for n in new_lst:
            for o in lst:
                if n[key] == o[key]:
                    for i in o:
                        o[i] = n[i]
                    break
            else:
                nn.append(n)
        lst.extend(nn)
        return lst

    @staticmethod
    def get_by_key(lst, key, value):
        for i in lst:
            if i[key] == value:
                return i
        else:
            return {}

    @staticmethod
    def export_as_csv(lst, dest_file):
        df = pd.DataFrame(lst)
        df.to_csv(dest_file)

    @staticmethod
    def get_pvpc_tariff(a_datetime):
        hdays = holidays.CountryHoliday("ES")
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


class Processor(ABC):
    _LABEL = "Processor"

    def __init__(self, input, settings={}, auto=True):
        self._input = deepcopy(input)
        self._settings = settings
        self._ready = False
        if auto:
            self.do_process()

    @abstractmethod
    def do_process(self):
        pass

    @property
    def output(self):
        return deepcopy(self._output)


class ConsumptionProcessor(Processor):
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
                {"date_format": "%Y-%m", "period": "M", "dictkey": "monthly"},
                {"date_format": "%Y-%m-%d", "period": "D", "dictkey": "daily"},
            ]:
                _t = self._df.copy()
                for p in ["p1", "p2", "p3"]:
                    _t["value_" + p + "_kWh"] = _t.loc[_t["px"] == p, "value_kWh"]
                _t.drop(["real"], axis=1, inplace=True)
                _t = _t.groupby([_t.datetime.dt.to_period(opt["period"])]).sum()
                _t.reset_index(inplace=True)
                _t["datetime"] = _t["datetime"].dt.strftime(opt["date_format"])
                _t = _t.round(2)
                self._output[opt["dictkey"]] = _t.to_dict("records")
            self._ready = True
        else:
            _LOGGER.warning(f"{self._LABEL} wrong data structure")
            return False


class MaximeterProcessor(Processor):
    _LABEL = "MaximeterProcessor"

    def do_process(self):
        self._output = {"stats": {}}
        self._df = pd.DataFrame(self._input)
        if all(k in self._df for k in ("datetime", "value_kW")):
            idx = self._df["value_kW"].argmax()
            self._output["stats"] = {
                "value_max_kW": self._df["value_kW"][idx],
                "date_max": f"{self._df['datetime'][idx]}",
                "value_mean_kW": self._df["value_kW"].mean(),
                "value_tile90_kW": self._df["value_kW"].quantile(0.9),
            }
        else:
            _LOGGER.warning(f"{self._LABEL} wrong data structure")
            return False


class BillingProcessor:
    _LABEL = "BillingProcessor"

    rules = {
        "p1_kw_year_eur": 30.67266,  # €/kW/year
        "p2_kw_year_eur": 1.4243591,  # €/kW/year
        "meter_month_eur": 0.81,  # €/month
        "market_kw_year_eur": 3.113,  # €/kW/año
        "electricity_tax": 1.0511300560,  # multiplicative
        "iva_tax": 1.1,  # multiplicative
    }

    def __init__(self, consumptions_lst, contracts_lst, prices_lst, rules={}):
        self.preprocess(consumptions_lst, contracts_lst, prices_lst)
        for i in rules:
            self.rules[i] = rules[i]

    def preprocess(self, consumptions_lst, contracts_lst, prices_lst):
        self.valid_data = False
        c_df = pd.DataFrame(consumptions_lst)
        if all(k in c_df for k in ("datetime", "value_kWh")):
            c_df["datetime"] = pd.to_datetime(c_df["datetime"])
            df = c_df
            p_df = pd.DataFrame(prices_lst)
            if all(k in p_df for k in ("datetime", "price")):
                p_df["datetime"] = pd.to_datetime(p_df["datetime"])
                df = df.merge(
                    p_df, how="left", left_on=["datetime"], right_on=["datetime"]
                )
                c = []
                try:
                    for contract in contracts_lst:
                        start = contract["date_start"]
                        end = contract["date_end"]
                        finish = False
                        while not finish:
                            c.append(
                                {
                                    "datetime": start,
                                    "power_p1": contract["power_p1"],
                                    "power_p2": contract["power_p2"]
                                    if contract["power_p2"] is not None
                                    else contract["power_p1"],
                                }
                            )
                            start = start + timedelta(hours=1)
                            finish = not (end > start)
                    df = df.merge(
                        pd.DataFrame(c),
                        how="left",
                        left_on=["datetime"],
                        right_on=["datetime"],
                    )
                    df["datetime"] = pd.to_datetime(df["datetime"])
                    df["electricity_taxfree"] = df["price"] * df["value_kWh"]
                    df["e_wtax"] = (
                        df["electricity_taxfree"]
                        * self.rules["electricity_tax"]
                        * self.rules["iva_tax"]
                    )
                    hprice_p1 = self.rules["p1_kw_year_eur"] / 365 / 24
                    hprice_p2 = self.rules["p2_kw_year_eur"] / 365 / 24
                    hprice_market = self.rules["market_kw_year_eur"] / 365 / 24
                    df["p_taxfree"] = (
                        df["power_p1"] * (hprice_p1 + hprice_market)
                        + df["power_p2"] * hprice_p2
                    )
                    df["p_wtax"] = (
                        df["p_taxfree"]
                        * self.rules["electricity_tax"]
                        * self.rules["iva_tax"]
                    )
                    self.df = df
                    self.valid_data = True
                except Exception as e:
                    _LOGGER.warning(f"{self._LABEL} wrong contracts data structure")
                    _LOGGER.exception(e)
            else:
                _LOGGER.warning(f"{self._LABEL} wrong prices data structure")
        else:
            _LOGGER.warning(f"{self._LABEL} wrong consumptions data structure")

    def process_range(self, dt_from=None, dt_to=None):
        dt_from = datetime(1970, 1, 1) if dt_from is None else dt_from
        dt_to = datetime.now() if dt_to is None else dt_to
        data = {}
        if self.valid_data:
            _df = self.df
            _t = _df.loc[
                (pd.to_datetime(dt_from) <= _df["datetime"])
                & (_df["datetime"] < pd.to_datetime(dt_to))
            ].copy()
            data = {
                "energy_term": round(_t["e_wtax"].sum(), 2),
                "power_term": round(_t["p_wtax"].sum(), 2),
                "other_terms": round(
                    self.rules["iva_tax"]
                    * ((dt_to - dt_from).total_seconds() / (24 * 3600))
                    * self.rules["meter_month_eur"]
                    / 30,
                    2,
                ),
            }
            data["total"] = (
                data["energy_term"] + data["power_term"] + data["other_terms"]
            )
        return data
