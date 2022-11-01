"""Billing data processors"""

import logging
from datetime import timedelta

import pandas as pd

from ..processors.base import Processor
from ..definitions import ConsumptionData, PricingRules, PricingData, check_integrity
from ..processors import utils

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


DEFAULT_RULES = PricingRules(
    p1_kw_year_eur=30.67266,
    p2_kw_year_eur=1.4243591,
    meter_month_eur=0.81,
    market_kw_year_eur=3.113,
    electricity_tax=1.0511300560,
    iva_tax=1.05,
)


class BillingProcessor(Processor):
    """A billing processor for edata"""

    _LABEL = "BillingProcessor"

    def do_process(self):
        """Prepares the costs dataframe"""
        self._output = {"hourly": [], "daily": [], "monthly": []}
        consumptions_lst = self._input["consumptions"]
        contracts_lst = self._input["contracts"]
        prices_lst = self._input["prices"]
        self.rules = self._input.get("rules", DEFAULT_RULES)
        c_df = pd.DataFrame(consumptions_lst)
        if check_integrity(c_df, ConsumptionData):
            c_df["datetime"] = pd.to_datetime(c_df["datetime"])
            _df = c_df
            p_df = pd.DataFrame(prices_lst)
            if check_integrity(p_df, PricingData):
                p_df["datetime"] = pd.to_datetime(p_df["datetime"])
                _df = _df.merge(
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
                except Exception:
                    _LOGGER.warning("Wrong contracts data structure")
                    return None

                _df = _df.merge(
                    pd.DataFrame(c),
                    how="left",
                    left_on=["datetime"],
                    right_on=["datetime"],
                )
                _df["datetime"] = pd.to_datetime(_df["datetime"])
                _df["energy_cost_raw"] = _df["value_eur_kWh"] * _df["value_kWh"]
                _df["energy_term"] = (
                    _df["energy_cost_raw"]
                    * self.rules["electricity_tax"]
                    * self.rules["iva_tax"]
                )
                hprice_p1 = self.rules["p1_kw_year_eur"] / 365 / 24
                hprice_p2 = self.rules["p2_kw_year_eur"] / 365 / 24
                hprice_market = self.rules["market_kw_year_eur"] / 365 / 24
                _df["power_cost_raw"] = (
                    _df["power_p1"] * (hprice_p1 + hprice_market)
                    + _df["power_p2"] * hprice_p2
                )
                _df["power_term"] = (
                    _df["power_cost_raw"]
                    * self.rules["electricity_tax"]
                    * self.rules["iva_tax"]
                )
                _df["others_term"] = (
                    self.rules["iva_tax"] * self.rules["meter_month_eur"] / 30
                )
                _df["value_eur"] = (
                    _df["energy_term"] + _df["power_term"] + _df["others_term"]
                )
                self._df = _df[
                    [
                        "datetime",
                        "value_eur",
                        "energy_term",
                        "power_term",
                        "others_term",
                    ]
                ]
                self._output["hourly"] = self._df.round(3).to_dict("records")
                for opt in [
                    {
                        "date_format": "%Y-%m-01T00:00:00",
                        "period": "M",
                        "dictkey": "monthly",
                    },
                    {
                        "date_format": "%Y-%m-%dT00:00:00",
                        "period": "D",
                        "dictkey": "daily",
                    },
                ]:
                    _t = self._df.copy()
                    _t = _t.groupby([_t.datetime.dt.to_period(opt["period"])]).sum()
                    _t.reset_index(inplace=True)
                    _t["datetime"] = _t["datetime"].dt.strftime(opt["date_format"])
                    _t = _t.round(2)
                    self._output[opt["dictkey"]] = utils.deserialize_dict(
                        _t.to_dict("records")
                    )
            else:
                _LOGGER.warning("Wrong prices data structure")
        else:
            _LOGGER.warning("Wrong consumptions data structure")
