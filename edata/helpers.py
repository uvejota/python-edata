"""A module for edata helpers"""

import asyncio
import logging
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional

import requests
from dateutil.relativedelta import relativedelta

from .connectors.datadis import DatadisConnector
from .connectors.redata import REDataConnector
from .definitions import ATTRIBUTES, EdataData, PricingRules
from .processors import utils
from .processors.billing import BillingInput, BillingProcessor
from .processors.consumption import ConsumptionProcessor
from .processors.maximeter import MaximeterProcessor

_LOGGER = logging.getLogger(__name__)


class EdataHelper:
    """Main EdataHelper class"""

    UPDATE_INTERVAL = timedelta(hours=1)

    def __init__(
        self,
        datadis_username: str,
        datadis_password: str,
        cups: str,
        datadis_authorized_nif: str = None,
        pricing_rules: PricingRules = None,
        data: EdataData = None,
    ) -> None:

        self.data = EdataData(
            supplies=[],
            contracts=[],
            consumptions=[],
            maximeter=[],
            pvpc=[],
            consumptions_daily_sum=[],
            consumptions_monthly_sum=[],
            cost_hourly_sum=[],
            cost_daily_sum=[],
            cost_monthly_sum=[],
        )
        self.attributes = {}

        self._cups = cups
        self._authorized_nif = datadis_authorized_nif
        self.last_update = {x: datetime(1970, 1, 1) for x in self.data.keys()}

        if data is not None:
            for i in [x for x in self.data.keys() if x in data]:
                self.data[i] = deepcopy(data[i])
        for attr in ATTRIBUTES:
            self.attributes[attr] = None

        self.datadis_api = DatadisConnector(
            datadis_username,
            datadis_password,
        )
        self.redata_api = REDataConnector()

        self.pricing_rules = pricing_rules

        if self.pricing_rules is not None:
            self.enable_billing = True
            if not all(
                x in self.pricing_rules and self.pricing_rules[x] is not None
                for x in ("p1_kwh_eur", "p2_kwh_eur", "p3_kwh_eur")
            ):
                self.is_pvpc = True
            else:
                self.is_pvpc = False
        else:
            self.enable_billing = False
            self.is_pvpc = False

    async def async_update(
        self,
        date_from: datetime = datetime(1970, 1, 1),
        date_to: datetime = datetime.today(),
    ):
        """Async call of update method"""
        asyncio.get_event_loop().run_in_executor(
            None, self.update, *[date_from, date_to]
        )

    def update(
        self,
        date_from: datetime = datetime(1970, 1, 1),
        date_to: datetime = datetime.today(),
    ):
        """Synchronous update"""

        # update datadis resources
        self.update_datadis(self._cups, date_from, date_to)

        # update redata resources if pvpc is requested
        if self.is_pvpc:
            try:
                self.update_redata(date_from, date_to)
            except requests.exceptions.Timeout:
                _LOGGER.error("Timeout exception while updating from REData")

        self.process_data()

    def update_supplies(self):
        """Synchronous data update of supplies"""
        if datetime.today().date() != self.last_update["supplies"].date():
            # if supplies haven't been updated today
            supplies = self.datadis_api.get_supplies(
                authorized_nif=self._authorized_nif
            )  # fetch supplies
            if len(supplies) > 0:
                self.data["supplies"] = supplies
                # if we got something, update last_update flag
                self.last_update["supplies"] = datetime.now()
                _LOGGER.info("Supplies data has been successfully updated")

    def update_contracts(self, cups: str, distributor_code: str):
        """Synchronous data update of contracts"""
        if datetime.today().date() != self.last_update["contracts"].date():
            # if contracts haven't been updated today
            contracts = self.datadis_api.get_contract_detail(
                cups, distributor_code, authorized_nif=self._authorized_nif
            )
            if len(contracts) > 0:
                self.data["contracts"] = utils.extend_by_key(
                    self.data["contracts"], contracts, "date_start"
                )  # extend contracts data with new ones
                # if we got something, update last_update flag
                self.last_update["contracts"] = datetime.now()
                _LOGGER.info("Contracts data has been successfully updated")

    def update_consumptions(
        self,
        cups: str,
        distributor_code: str,
        start_date: datetime,
        end_date: datetime,
        measurement_type: str,
        point_type: int,
    ):
        """Synchronous data update of consumptions"""

        if (datetime.now() - self.last_update["consumptions"]) > self.UPDATE_INTERVAL:
            consumptions = self.datadis_api.get_consumption_data(
                cups,
                distributor_code,
                start_date,
                end_date,
                measurement_type,
                point_type,
                authorized_nif=self._authorized_nif,
            )
            if len(consumptions) > 0:
                self.data["consumptions"] = utils.extend_by_key(
                    self.data["consumptions"], consumptions, "datetime"
                )
                self.last_update["consumptions"] = datetime.now()
                _LOGGER.info(
                    "Consumptions data has been successfully updated (%s elements)",
                    len(consumptions),
                )

    def update_maximeter(self, cups, distributor_code, start_date, end_date):
        """Synchronous data update of maximeter"""
        if (datetime.now() - self.last_update["maximeter"]) > self.UPDATE_INTERVAL:
            maximeter = self.datadis_api.get_max_power(
                cups,
                distributor_code,
                start_date,
                end_date,
                authorized_nif=self._authorized_nif,
            )
            if len(maximeter) > 0:
                self.data["maximeter"] = utils.extend_by_key(
                    self.data["maximeter"], maximeter, "datetime"
                )
                self.last_update["maximeter"] = datetime.now()
                _LOGGER.info(
                    "Maximeter data has been successfully updated (%s elements)",
                    len(maximeter),
                )

    def update_datadis(
        self,
        cups: str,
        date_from: datetime = datetime(1970, 1, 1),
        date_to: datetime = datetime.today(),
    ):
        """Synchronous data update"""
        _LOGGER.info(
            "Update requested for CUPS %s from %s to %s",
            cups[-4:],
            date_from.isoformat(),
            date_to.isoformat(),
        )

        # update supplies and get distributorCode
        self.update_supplies()

        if len(self.data["supplies"]) == 0:
            # return if no supplies were discovered
            _LOGGER.warning(
                "Supplies query failed or no supplies found in the provided account"
            )
            return False

        # find requested cups in supplies
        supply = utils.get_by_key(self.data["supplies"], "cups", cups)
        if supply is None:
            # return if specified cups seems not valid
            _LOGGER.error(
                "CUPS %s not found in %s, wrong CUPS?",
                cups[-4:],
                [x["cups"] for x in self.data["supplies"]],
            )
            return False

        # get some supply-related data
        supply_date_start = supply["date_start"]
        distributor_code = supply["distributorCode"]
        point_type = supply["pointType"]

        # update contracts to get valid periods
        self.update_contracts(cups, distributor_code)
        if len(self.data["contracts"]) == 0:
            _LOGGER.warning(
                "Contracts query failed or no contracts found in the provided account"
            )
            return False

        # filter consumptions and maximeter, and look for gaps
        def sort_and_filter(dt_from, dt_to):
            self.data["consumptions"], miss_cons = utils.extract_dt_ranges(
                self.data["consumptions"],
                dt_from,
                dt_to,
                gap_interval=timedelta(hours=6),
            )
            self.data["maximeter"], miss_maxim = utils.extract_dt_ranges(
                self.data["maximeter"],
                dt_from,
                dt_to,
                gap_interval=timedelta(days=60),
            )
            return miss_cons, miss_maxim

        miss_cons, miss_maxim = sort_and_filter(date_from, date_to)

        _LOGGER.info(
            "Identified missing consumptions: %s",
            ", ".join(
                [x["from"].isoformat() + " - " + x["to"].isoformat() for x in miss_cons]
            ),
        )
        _LOGGER.info(
            "Identified missing maximeter: %s",
            ", ".join(
                [
                    x["from"].isoformat() + " - " + x["to"].isoformat()
                    for x in miss_maxim
                ]
            ),
        )

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
                # fetch consumptions for each consumptions gap in valid periods
                self.update_consumptions(
                    cups,
                    distributor_code,
                    max([gap["from"], contract["date_start"]]),
                    min([gap["to"], contract["date_end"]]),
                    "0",
                    point_type,
                )

            # update maximeter
            for gap in [
                x
                for x in miss_cons
                if not (
                    date_to < contract["date_start"] or date_from > contract["date_end"]
                )
            ]:
                # fetch maximeter for each maximeter gap in valid periods
                start = max(
                    [gap["from"], contract["date_start"] + relativedelta(months=1)]
                )
                end = min([gap["to"], contract["date_end"]])
                start = min([start, end])
                self.update_maximeter(cups, distributor_code, start, end)

        # safe check periods in non-registered contracts
        if oldest_contract != supply_date_start and oldest_contract > max(
            [date_from, supply_date_start]
        ):
            _LOGGER.info(
                "Supplies and contract start date do not match, exploring non-registered contracts"
            )
            start = max([supply_date_start, date_from])
            self.update_consumptions(
                cups, distributor_code, start, oldest_contract, "0", point_type
            )
            self.update_maximeter(cups, distributor_code, start, oldest_contract)
            miss_cons, miss_maxim = sort_and_filter(start, date_to)
        else:
            miss_cons, miss_maxim = sort_and_filter(
                max([date_from, oldest_contract]), date_to
            )

        return True

    def update_redata(
        self,
        date_from: datetime = (datetime.today() - timedelta(days=30)).replace(
            hour=0, minute=0
        ),
        date_to: datetime = (datetime.today() + timedelta(days=2)).replace(
            hour=0, minute=0
        ),
    ):
        """Fetch PVPC prices using REData API"""

        self.data["pvpc"], missing = utils.extract_dt_ranges(
            self.data["pvpc"],
            date_from,
            date_to,
            gap_interval=timedelta(hours=1),
        )
        for gap in missing:
            prices = []
            gap["from"] = max(
                (datetime.today() - timedelta(days=30)).replace(hour=0, minute=0),
                gap["from"],
            )
            while len(prices) == 0 and gap["from"] < gap["to"]:
                prices = self.redata_api.get_realtime_prices(gap["from"], gap["to"])
                gap["from"] = gap["from"] + timedelta(days=1)
            self.data["pvpc"] = utils.extend_by_key(
                self.data["pvpc"], prices, "datetime"
            )

        return True

    def process_data(self):
        """Process all raw data"""
        for process_method in [
            self.process_supplies,
            self.process_contracts,
            self.process_consumptions,
            self.process_maximeter,
            self.process_cost,
        ]:
            try:
                process_method()
            except Exception as ex:  # pylint: disable=broad-except
                _LOGGER.error("Unhandled exception while updating attributes")
                _LOGGER.exception(ex)

        for attribute in self.attributes:
            if attribute in ATTRIBUTES and ATTRIBUTES[attribute] is not None:
                self.attributes[attribute] = (
                    round(self.attributes[attribute], 2)
                    if self.attributes[attribute] is not None
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

            yday = utils.get_by_key(
                daily,
                "datetime",
                today_starts - timedelta(days=1),
            )
            self.attributes["yesterday_kWh"] = (
                yday.get("value_kWh", None) if yday is not None else None
            )
            self.attributes["yesterday_p1_kWh"] = (
                yday.get("value_p1_kWh", None) if yday is not None else None
            )
            self.attributes["yesterday_p2_kWh"] = (
                yday.get("value_p2_kWh", None) if yday is not None else None
            )
            self.attributes["yesterday_p3_kWh"] = (
                yday.get("value_p3_kWh", None) if yday is not None else None
            )
            self.attributes["yesterday_hours"] = (
                yday.get("delta_h", None) if yday is not None else None
            )

            month = utils.get_by_key(monthly, "datetime", month_starts)
            self.attributes["month_kWh"] = (
                month.get("value_kWh", None) if month is not None else None
            )
            self.attributes["month_days"] = (
                month.get("delta_h", 0) / 24 if month is not None else None
            )
            self.attributes["month_daily_kWh"] = (
                (
                    (self.attributes["month_kWh"] / self.attributes["month_days"])
                    if self.attributes["month_days"] > 0
                    else 0
                )
                if month is not None
                else None
            )
            self.attributes["month_p1_kWh"] = (
                month.get("value_p1_kWh", None) if month is not None else None
            )
            self.attributes["month_p2_kWh"] = (
                month.get("value_p2_kWh", None) if month is not None else None
            )
            self.attributes["month_p3_kWh"] = (
                month.get("value_p3_kWh", None) if month is not None else None
            )

            last_month = utils.get_by_key(
                monthly,
                "datetime",
                (month_starts - relativedelta(months=1)),
            )
            self.attributes["last_month_kWh"] = (
                last_month.get("value_kWh", None) if last_month is not None else None
            )
            self.attributes["last_month_days"] = (
                last_month.get("delta_h", 0) / 24 if last_month is not None else None
            )
            self.attributes["last_month_daily_kWh"] = (
                (
                    (
                        self.attributes["last_month_kWh"]
                        / self.attributes["last_month_days"]
                    )
                    if self.attributes["last_month_days"] > 0
                    else 0
                )
                if last_month is not None
                else None
            )
            self.attributes["last_month_p1_kWh"] = (
                last_month.get("value_p1_kWh", None) if last_month is not None else None
            )
            self.attributes["last_month_p2_kWh"] = (
                last_month.get("value_p2_kWh", None) if last_month is not None else None
            )
            self.attributes["last_month_p3_kWh"] = (
                last_month.get("value_p3_kWh", None) if last_month is not None else None
            )

            if len(self.data["consumptions"]) > 0:
                self.attributes["last_registered_date"] = self.data["consumptions"][-1][
                    "datetime"
                ]

                last_day = utils.get_by_key(
                    daily,
                    "datetime",
                    self.attributes["last_registered_date"].replace(
                        hour=0, minute=0, second=0
                    ),
                )
                self.attributes["last_registered_day_kWh"] = (
                    last_day.get("value_kWh", None) if last_day is not None else None
                )
                self.attributes["last_registered_day_p1_kWh"] = (
                    last_day.get("value_p1_kWh", None) if last_day is not None else None
                )
                self.attributes["last_registered_day_p2_kWh"] = (
                    last_day.get("value_p2_kWh", None) if last_day is not None else None
                )
                self.attributes["last_registered_day_p3_kWh"] = (
                    last_day.get("value_p3_kWh", None) if last_day is not None else None
                )
                self.attributes["last_registered_day_hours"] = (
                    last_day.get("delta_h", None) if last_day is not None else None
                )

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

    def process_cost(self):
        """Process costs"""
        if self.enable_billing:
            proc = BillingProcessor(
                BillingInput(
                    contracts=self.data["contracts"],
                    consumptions=self.data["consumptions"],
                    prices=self.data["pvpc"] if self.is_pvpc else None,
                    rules=self.pricing_rules,
                )
            )
            month_starts = datetime(
                datetime.today().year, datetime.today().month, 1, 0, 0, 0
            )

            hourly = proc.output["hourly"]
            daily = proc.output["daily"]
            monthly = proc.output["monthly"]

            self.data["cost_hourly_sum"] = hourly
            self.data["cost_daily_sum"] = daily
            self.data["cost_monthly_sum"] = monthly

            this_month = utils.get_by_key(
                monthly,
                "datetime",
                month_starts,
            )

            last_month = utils.get_by_key(
                monthly,
                "datetime",
                (month_starts - relativedelta(months=1)),
            )

            if this_month is not None:
                self.attributes["month_€"] = this_month.get("value_eur", None)

            if last_month is not None:
                self.attributes["last_month_€"] = last_month.get("value_eur", None)
