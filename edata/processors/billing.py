"""Billing data processors"""

import logging
from collections.abc import Iterable
from datetime import timedelta
from types import SimpleNamespace
from typing import Optional, TypedDict

import pandas as pd

from ..definitions import (
    ConsumptionData,
    ContractData,
    PricingAggData,
    PricingData,
    PricingRules,
    check_integrity,
)
from ..processors import utils
from ..processors.base import Processor

_LOGGER = logging.getLogger(__name__)


class BillingOutput(TypedDict):
    """A dict holding BillingProcessor output property"""

    hourly: Iterable[PricingAggData]
    daily: Iterable[PricingAggData]
    monthly: Iterable[PricingAggData]


class BillingInput(TypedDict):
    """A dict holding BillingProcessor input data"""

    contracts: Iterable[ContractData]
    consumptions: Iterable[ConsumptionData]
    prices: Optional[Iterable[PricingData]]
    rules: PricingRules


class BillingProcessor(Processor):
    """A billing processor for edata"""

    def do_process(self):
        """Main method for the BillingProcessor"""
        self._output = BillingOutput(hourly=[], daily=[], monthly=[])

        _input = SimpleNamespace(**self._input)

        c_df = pd.DataFrame(_input.consumptions)
        if check_integrity(c_df, ConsumptionData):
            c_df.datetime = pd.to_datetime(c_df.datetime)
            _df = c_df
            if _input.prices is not None and len(_input.prices) > 0:
                p_df = pd.DataFrame(_input.prices)
            else:
                c_df["px"] = c_df["datetime"].apply(utils.get_pvpc_tariff)
                p_df = c_df[["datetime", "px"]].copy()
                p_df["value_eur_kWh"] = 0
                p_df["delta_h"] = 1

                for tariff in ("p1", "p2", "p3"):
                    p_df.loc[:, ("value_eur_kWh", "px")] = _input.rules[
                        tariff + "_kwh_eur"
                    ]
                p_df.drop("px", axis=1, inplace=True)
            if check_integrity(p_df, PricingData):
                p_df.datetime = pd.to_datetime(p_df.datetime)
                _df = _df.merge(
                    p_df, how="left", left_on=["datetime"], right_on=["datetime"]
                )
                contracts = []
                for contract in _input.contracts:
                    if check_integrity(contract, ContractData):
                        start = contract["date_start"]
                        end = contract["date_end"]
                        finish = False
                        while not finish:
                            contracts.append(
                                ContractData(
                                    datetime=start,
                                    power_p1=contract["power_p1"],
                                    power_p2=contract["power_p2"]
                                    if contract["power_p2"] is not None
                                    else contract["power_p1"],
                                )
                            )
                            start = start + timedelta(hours=1)
                            finish = not (end > start)
                    else:
                        _LOGGER.warning("Wrong contracts data structure")

                _df = _df.merge(
                    pd.DataFrame(contracts),
                    how="left",
                    left_on=["datetime"],
                    right_on=["datetime"],
                )
                _df.datetime = pd.to_datetime(_df.datetime)
                _df["energy_cost_raw"] = _df.value_eur_kWh * _df.value_kWh
                _df["energy_term"] = (
                    _df.energy_cost_raw
                    * _input.rules["electricity_tax"]
                    * _input.rules["iva_tax"]
                )
                hprice_p1 = _input.rules["p1_kw_year_eur"] / 365 / 24
                hprice_p2 = _input.rules["p2_kw_year_eur"] / 365 / 24
                hprice_market = _input.rules["market_kw_year_eur"] / 365 / 24
                _df["power_cost_raw"] = (
                    _df.power_p1 * (hprice_p1 + hprice_market)
                    + _df.power_p2 * hprice_p2
                )
                _df["power_term"] = (
                    _df.power_cost_raw
                    * _input.rules["electricity_tax"]
                    * _input.rules["iva_tax"]
                )
                _df["others_term"] = (
                    _input.rules["iva_tax"] * _input.rules["meter_month_eur"] / 30 / 24
                )
                _df["value_eur"] = _df.energy_term + _df.power_term + _df.others_term
                _df = _df[_df.value_eur.notnull()]
                self._df = _df[
                    [
                        "datetime",
                        "value_eur",
                        "energy_term",
                        "power_term",
                        "others_term",
                    ]
                ]
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
                    {
                        "date_format": "%Y-%m-%dT00:00:00",
                        "period": "D",
                        "dictkey": "daily",
                    },
                ):
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
