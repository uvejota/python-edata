"""A module for edata helpers"""

import asyncio
import logging
from copy import deepcopy
from datetime import datetime, timedelta

import requests
from dateutil.relativedelta import relativedelta

from .connectors import DatadisConnector
from .processors import *

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["datadis"]
ATTRIBUTES = {
    "cups": None,
    "contract_p1_kW": "kW",
    "contract_p2_kW": "kW",
    "yesterday_kWh": "kWh",
    "yesterday_hours": "h",
    "yesterday_p1_kWh": "kWh",
    "yesterday_p2_kWh": "kWh",
    "yesterday_p3_kWh": "kWh",
    "month_kWh": "kWh",
    "month_daily_kWh": "kWh",
    "month_days": "d",
    "month_p1_kWh": "kWh",
    "month_p2_kWh": "kWh",
    "month_p3_kWh": "kWh",
    # "month_pvpc_€": '€',
    "last_month_kWh": "kWh",
    "last_month_daily_kWh": "kWh",
    "last_month_days": "d",
    "last_month_p1_kWh": "kWh",
    "last_month_p2_kWh": "kWh",
    "last_month_p3_kWh": "kWh",
    # "last_month_pvpc_€": '€',
    # "last_month_idle_W": 'W',
    "max_power_kW": "kW",
    "max_power_date": None,
    "max_power_mean_kW": "kW",
    "max_power_90perc_kW": "kW",
    "last_registered_kWh_date": None,
}

EXPERIMENTAL_ATTRS = []


class EdataHelper:
    """Main EdataHelper class"""

    SCOPE = ["supplies", "contracts", "consumptions", "maximeter"]
    SECURE_FETCH_THRESHOLD = 1
    UPDATE_INTERVAL = timedelta(minutes=60)

    def __init__(
        self,
        username,
        password,
        cups,
        authorized_nif=None,
        data=None,
        experimental=False,
        log_level=logging.WARNING,
    ) -> None:

        logging.getLogger().setLevel(log_level)

        self.data = {
            "supplies": [],
            "contracts": [],
            "consumptions": [],
            "maximeter": [],
            "pvpc": [],
        }
        self.attributes = {}
        self._cups = cups
        self._experimental = experimental
        self._retries = 0
        self._last_try = datetime(1970, 1, 1)
        self._authorized_nif = authorized_nif
        self.last_update = {x: datetime(1970, 1, 1) for x in self.SCOPE}

        if data is not None:
            for i in [x for x in self.SCOPE if x in data]:
                self.data[i] = deepcopy(data[i])
        for attr in ATTRIBUTES:
            if self._experimental or attr not in EXPERIMENTAL_ATTRS:
                self.attributes[attr] = None

        self.datadis_api = DatadisConnector(username, password, log_level=log_level)

        for attr in ATTRIBUTES:
            if self._experimental or attr not in EXPERIMENTAL_ATTRS:
                self.attributes[attr] = None

    async def async_update(
        self, date_from=datetime(1970, 1, 1), date_to=datetime.today()
    ):
        """Async call of update method"""
        asyncio.get_event_loop().run_in_executor(
            None, self.update, *[date_from, date_to]
        )

    def update(
        self,
        date_from=datetime(1970, 1, 1),
        date_to=datetime.today(),
    ):
        """Synchronous update"""
        try:
            self.update_datadis(self._cups, date_from, date_to)
        except requests.exceptions.Timeout:
            _LOGGER.error("Timeout while updating from datadis")

        self.process_data()

    def update_supplies(self):
        """Synchronous data update of supplies"""
        supplies = (
            self.datadis_api.get_supplies(authorized_nif=self._authorized_nif)
            if (datetime.today().date() != self.last_update["supplies"].date())
            or (len(self.data["supplies"]) == 0)
            else []
        )
        if len(supplies) > 0:
            self.data["supplies"] = supplies
            self.last_update["supplies"] = datetime.now()
            _LOGGER.info(
                "Supplies data has been successfully updated (%s elements)",
                len(supplies),
            )
        else:
            _LOGGER.debug("supplies data was not updated")

    def update_contracts(self, cups, distributor_code):
        """Synchronous data update of contracts"""
        contracts = (
            self.datadis_api.get_contract_detail(
                cups, distributor_code, authorized_nif=self._authorized_nif
            )
            if (datetime.today().date() != self.last_update["contracts"].date())
            or (len(self.data["contracts"]) == 0)
            else []
        )
        if len(contracts) > 0:
            self.data["contracts"] = DataUtils.extend_by_key(
                self.data["contracts"], contracts, "date_start"
            )
            self.last_update["contracts"] = datetime.now()
            _LOGGER.info(
                "Contracts data has been successfully updated (%s elements)",
                len(contracts),
            )
        else:
            _LOGGER.debug("contracts data was not updated")

    def update_consumptions(
        self, cups, distributor_code, start_date, end_date, measurement_type, point_type
    ):
        """Synchronous data update of consumptions"""
        if (
            self._retries >= self.SECURE_FETCH_THRESHOLD
            and (end_date - start_date).days > 31
        ):

            def total_months(input_datetime):
                return input_datetime.month + 12 * input_datetime.year

            mlist = []
            for tot_m in range(total_months(start_date) - 1, total_months(end_date)):
                y, m = divmod(tot_m, 12)
                mlist.append(datetime(y, m + 1, 1))
            for m in mlist:
                self.update_consumptions(
                    cups,
                    distributor_code,
                    max(m, start_date),
                    min(m + relativedelta(months=1), end_date),
                    measurement_type,
                    point_type,
                )
        else:
            r = self.datadis_api.get_consumption_data(
                cups,
                distributor_code,
                start_date,
                end_date,
                measurement_type,
                point_type,
                authorized_nif=self._authorized_nif,
            )
            if len(r) > 0:
                self.data["consumptions"] = DataUtils.extend_by_key(
                    self.data["consumptions"], r, "datetime"
                )
                self.last_update["consumptions"] = datetime.now()
                _LOGGER.info(
                    "Consumptions data has been successfully updated (%s elements)",
                    len(r),
                )
            else:
                _LOGGER.debug("consumptions data was not updated")

    def update_maximeter(self, cups, distributor_code, start_date, end_date):
        """Synchronous data update of maximeter"""
        r = self.datadis_api.get_max_power(cups, distributor_code, start_date, end_date)
        if len(r) > 0:
            self.data["maximeter"] = DataUtils.extend_by_key(
                self.data["maximeter"], r, "datetime"
            )
            self.last_update["maximeter"] = datetime.now()
            _LOGGER.info(
                "Maximeter data has been successfully updated (%s elements)", len(r)
            )
        else:
            _LOGGER.debug("maximeter data was not updated")

    def update_datadis(
        self,
        cups,
        date_from=datetime(1970, 1, 1),
        date_to=datetime.today(),
        ignore_interval=False,
    ):
        """Synchronous data update"""
        _LOGGER.info(
            "Update requested for CUPS %s from %s to %s", cups[-4:], date_from, date_to
        )

        if (
            not ignore_interval
            and (datetime.now() - self._last_try) < self.UPDATE_INTERVAL
        ):
            _LOGGER.info("Skipping due to update interval")
            return False
        else:
            self._last_try = datetime.now()

        # update supplies and get distributorCode
        self.update_supplies()

        for s in self.data["supplies"]:
            if s["cups"] == cups:
                s_start = s["date_start"]
                dcode = s["distributorCode"]
                ptype = s["pointType"]
                break
        else:
            if len(self.data["supplies"]) == 0:
                _LOGGER.warning(
                    "Supplies query failed or no supplies found in the provided account, retry later"
                )
            else:
                _LOGGER.error(
                    "CUPS %s not found in %s, wrong CUPS?",
                    cups[-4:],
                    [x["cups"] for x in self.data["supplies"]],
                )
            return False

        # update contracts to get valid periods
        self.update_contracts(cups, dcode)
        if len(self.data["contracts"]) == 0:
            _LOGGER.warning("Contracts query failed, retry later")
            return False

        # filter consumptions and maximeter, and look for gaps
        def sort_and_filter(dt_from, dt_to):
            self.data["consumptions"], miss_cons = DataUtils.extract_dt_ranges(
                self.data["consumptions"],
                dt_from,
                dt_to,
                gap_interval=timedelta(hours=6),
            )
            self.data["maximeter"], miss_maxim = DataUtils.extract_dt_ranges(
                self.data["maximeter"],
                dt_from,
                dt_to,
                gap_interval=timedelta(days=60),
            )
            return miss_cons, miss_maxim

        miss_cons, miss_maxim = sort_and_filter(date_from, date_to)

        oldest_contract = datetime.today()
        for contract in self.data["contracts"]:
            # register oldest contract
            if contract["date_start"] < oldest_contract:
                oldest_contract = contract["date_start"]

            # update consumptions
            for gap in [
                x
                for x in miss_cons
                if not (
                    x["to"] < contract["date_start"] or x["from"] > contract["date_end"]
                )
            ]:
                start = max([gap["from"], contract["date_start"]])
                end = min([gap["to"], contract["date_end"]])
                self.update_consumptions(cups, dcode, start, end, "0", ptype)

            # update maximeter
            for gap in [
                x
                for x in miss_cons
                if not (
                    date_to < contract["date_start"] or date_from > contract["date_end"]
                )
            ]:
                start = max(
                    [gap["from"], contract["date_start"] + relativedelta(months=1)]
                )
                end = min([gap["to"], contract["date_end"]])
                start = min([start, end])
                self.update_maximeter(cups, dcode, start, end)

        # safe check periods in non-registered contracts
        if oldest_contract != s_start and oldest_contract > max([date_from, s_start]):
            start = max([s_start, date_from])
            self.update_consumptions(cups, dcode, start, oldest_contract, "0", ptype)
            self.update_maximeter(cups, dcode, start, oldest_contract)
            miss_cons, miss_maxim = sort_and_filter(start, date_to)
        else:
            miss_cons, miss_maxim = sort_and_filter(
                max([date_from, oldest_contract]), date_to
            )

        if len(miss_cons) > 1:
            self._retries += 1
            if not ignore_interval:
                _LOGGER.info(
                    "Still missing the following consumption ranges %s, retrying",
                    miss_cons,
                )
                return self.update_datadis(
                    cups, date_from, date_to, ignore_interval=True
                )

            _LOGGER.warning(
                "Still missing the following consumption ranges %s, will try again later",
                miss_cons,
            )
        else:
            self._retries = 0

        return True

    def process_data(self):
        """Process all raw data"""
        for f in [
            self.process_supplies,
            self.process_contracts,
            self.process_consumptions,
            self.process_maximeter,
        ]:
            try:
                f()
            except Exception as ex:
                _LOGGER.error("Unhandled exception while updating attributes")
                _LOGGER.exception(ex)

        for a in self.attributes:
            if a in ATTRIBUTES and ATTRIBUTES[a] is not None:
                self.attributes[a] = (
                    round(self.attributes[a], 2)
                    if self.attributes[a] is not None
                    else None
                )

    def process_supplies(self):
        """Process supplies data"""
        for i in self.data["supplies"]:
            if i["cups"] == self._cups:
                self.attributes["cups"] = self._cups
                break

    def process_contracts(self):
        """Process contracts data"""
        most_recent_date = datetime(1970, 1, 1)
        for i in self.data["contracts"]:
            if i["date_end"] > most_recent_date:
                most_recent_date = i["date_end"]
                self.attributes["contract_p1_kW"] = i.get("power_p1", None)
                self.attributes["contract_p2_kW"] = i.get("power_p2", None)
                break

    def process_consumptions(self):
        """Process consumptions data"""
        if len(self.data["consumptions"]) > 0:
            proc = ConsumptionProcessor(self.data["consumptions"])

            today_starts = datetime(
                datetime.today().year,
                datetime.today().month,
                datetime.today().day,
                0,
                0,
                0,
            )

            month_starts = datetime(
                datetime.today().year, datetime.today().month, 1, 0, 0, 0
            )

            # hourly = proc.output['hourly']
            daily = proc.output["daily"]
            monthly = proc.output["monthly"]

            self.data["consumptions_daily_sum"] = daily
            self.data["consumptions_monthly_sum"] = monthly

            yday = DataUtils.get_by_key(
                daily,
                "datetime",
                (today_starts.date() - timedelta(days=1)).strftime("%Y-%m-%d"),
            )
            self.attributes["yesterday_kWh"] = yday.get("value_kWh", None)
            self.attributes["yesterday_p1_kWh"] = yday.get("value_p1_kWh", None)
            self.attributes["yesterday_p2_kWh"] = yday.get("value_p2_kWh", None)
            self.attributes["yesterday_p3_kWh"] = yday.get("value_p3_kWh", None)
            self.attributes["yesterday_hours"] = yday.get("delta_h", None)

            month = DataUtils.get_by_key(
                monthly, "datetime", month_starts.strftime("%Y-%m")
            )
            self.attributes["month_kWh"] = month.get("value_kWh", None)
            self.attributes["month_days"] = month.get("delta_h", 0) / 24
            self.attributes["month_daily_kWh"] = (
                (self.attributes["month_kWh"] / self.attributes["month_days"])
                if self.attributes["month_days"] > 0
                else 0
            )
            self.attributes["month_p1_kWh"] = month.get("value_p1_kWh", None)
            self.attributes["month_p2_kWh"] = month.get("value_p2_kWh", None)
            self.attributes["month_p3_kWh"] = month.get("value_p3_kWh", None)

            last_month = DataUtils.get_by_key(
                monthly,
                "datetime",
                (month_starts - relativedelta(months=1)).strftime("%Y-%m"),
            )
            self.attributes["last_month_kWh"] = last_month.get("value_kWh", None)
            self.attributes["last_month_days"] = last_month.get("delta_h", 0) / 24
            self.attributes["last_month_daily_kWh"] = (
                (self.attributes["last_month_kWh"] / self.attributes["last_month_days"])
                if self.attributes["last_month_days"] > 0
                else 0
            )
            self.attributes["last_month_p1_kWh"] = last_month.get("value_p1_kWh", None)
            self.attributes["last_month_p2_kWh"] = last_month.get("value_p2_kWh", None)
            self.attributes["last_month_p3_kWh"] = last_month.get("value_p3_kWh", None)

            self.attributes["last_registered_kWh_date"] = self.data["consumptions"][-1][
                "datetime"
            ]

    def process_maximeter(self):
        """Process maximeter data"""
        if len(self.data["maximeter"]) > 0:
            processor = MaximeterProcessor(self.data["maximeter"])
            last_relative_year = processor.output["stats"]
            self.attributes["max_power_kW"] = last_relative_year.get(
                "value_max_kW", None
            )
            self.attributes["max_power_date"] = last_relative_year.get("date_max", None)
            self.attributes["max_power_mean_kW"] = last_relative_year.get(
                "value_mean_kW", None
            )
            self.attributes["max_power_90perc_kW"] = last_relative_year.get(
                "value_tile90_kW", None
            )

    def __str__(self) -> str:
        return "\n".join([f"{i}: {self.attributes[i]}" for i in self.attributes])
