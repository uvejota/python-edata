"""A module for edata helpers."""

import asyncio
import contextlib
from datetime import datetime, timedelta
import logging
import os

from dateutil.relativedelta import relativedelta
import requests

from edata.services.data_service import DataService

from . import const
from .providers.redata import REDataConnector
from .definitions import (
    ATTRIBUTES,
    ConsumptionAggData,
    ConsumptionData,
    ContractData,
    EdataData,
    MaxPowerData,
    PricingRules,
    SupplyData,
)
from .processors import utils
from .processors.billing import BillingInput, BillingProcessor
from .processors.maximeter import MaximeterProcessor
from .storage import check_storage_integrity, dump_storage, load_storage

_LOGGER = logging.getLogger(__name__)


def acups(cups):
    """Print an abbreviated and anonymized CUPS."""

    return cups[-5:]


class EdataHelper:
    """Main EdataHelper class."""

    UPDATE_INTERVAL = timedelta(hours=1)

    def __init__(
        self,
        datadis_username: str,
        datadis_password: str,
        cups: str,
        datadis_authorized_nif: str | None = None,
        pricing_rules: PricingRules | None = None,
        storage_dir_path: str | None = None,
        data: EdataData | None = None,
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
        self._storage_dir = storage_dir_path
        self._cups = cups
        self._scups = acups(cups)
        self._authorized_nif = datadis_authorized_nif
        self.last_update = {x: datetime(1970, 1, 1) for x in self.data}
        self._date_from = datetime(1970, 1, 1)
        self._date_to = datetime.today()
        self._must_dump = True
        self._incremental_update = True

        if data is not None:
            data = check_storage_integrity(data)
            self.data = data
        else:
            with contextlib.suppress(Exception):
                self.data = load_storage(self._cups, self._storage_dir)

        for attr in ATTRIBUTES:
            self.attributes[attr] = None

        self.data_service = DataService(
            cups,
            datadis_username,
            datadis_password,
            datadis_authorized_nif,
            storage_path=(
                os.path.join(storage_dir_path, const.PROG_NAME)
                if storage_dir_path is not None
                else None
            ),
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
        incremental_update: bool = True,
    ):
        """Async call of update method."""

        _LOGGER.info(
            "%s: update triggered",
            self._scups,
        )
        self._date_from = date_from
        self._date_to = date_to

        # update datadis resources
        # TODO replace with DataService
        await self.data_service.update(date_from, date_to)

        self.data["supplies"] = [
            SupplyData(**x.model_dump()) for x in self.data_service._supplies
        ]
        self.data["contracts"] = [
            ContractData(**x.model_dump()) for x in self.data_service._contracts
        ]
        self.data["consumptions"] = [
            ConsumptionData(**x.model_dump()) for x in self.data_service._energy
        ]
        self.data["maximeter"] = [
            MaxPowerData(**x.model_dump()) for x in self.data_service._power
        ]
        self.data["consumptions_daily_sum"] = [
            ConsumptionAggData(**x.model_dump())
            for x in await self.data_service.get_statistics("day")
        ]
        self.data["consumptions_monthly_sum"] = [
            ConsumptionAggData(**x.model_dump())
            for x in await self.data_service.get_statistics("month")
        ]

        # update redata resources if pvpc is requested
        if self.is_pvpc:
            try:
                await self.update_redata(date_from, date_to)
            except requests.exceptions.Timeout:
                _LOGGER.error("Timeout exception while updating from REData")

        await asyncio.to_thread(
            self.process_data, incremental_update=incremental_update
        )

        if self._must_dump:
            await asyncio.to_thread(
                dump_storage, self._cups, self.data, self._storage_dir
            )

    def update(
        self,
        date_from: datetime = datetime(1970, 1, 1),
        date_to: datetime = datetime.today(),
        incremental_update: bool = True,
    ):
        """Update synchronously."""

        asyncio.run(self.async_update(date_from, date_to, incremental_update))

    async def update_redata(
        self,
        date_from: datetime = (datetime.today() - timedelta(days=30)).replace(
            hour=0, minute=0
        ),
        date_to: datetime = (datetime.today() + timedelta(days=2)).replace(
            hour=0, minute=0
        ),
    ):
        """Fetch PVPC prices using REData API."""

        _LOGGER.info(
            "%s: updating PVPC prices",
            self._scups,
        )

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
                prices = await self.redata_api.async_get_realtime_prices(
                    gap["from"], gap["to"]
                )
                gap["from"] = gap["from"] + timedelta(days=1)
            self.data["pvpc"] = utils.extend_by_key(
                self.data["pvpc"], prices, "datetime"
            )

        return True

    def process_data(self, incremental_update: bool = True):
        """Process all raw data."""

        self._incremental_update = incremental_update

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

        if not incremental_update:
            dump_storage(self._cups, self.data, self._storage_dir)

    def process_supplies(self):
        """Process supplies data."""
        for i in self.data["supplies"]:
            if i["cups"] == self._cups:
                self.attributes["cups"] = self._cups
                break

    def process_contracts(self):
        """Process contracts data."""
        most_recent_date = datetime(1970, 1, 1)
        for i in self.data["contracts"]:
            if i["date_end"] > most_recent_date:
                most_recent_date = i["date_end"]
                self.attributes["contract_p1_kW"] = i.get("power_p1", None)
                self.attributes["contract_p2_kW"] = i.get("power_p2", None)
                break

    def process_consumptions(self):
        """Process consumptions data."""
        if len(self.data["consumptions"]) > 0:

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

            yday = utils.get_by_key(
                self.data["consumptions_daily_sum"],
                "datetime",
                today_starts - timedelta(days=1),
            )
            self.attributes["yesterday_kWh"] = (
                yday.get("value_kWh", None) if yday is not None else None
            )

            for tariff in (1, 2, 3):
                self.attributes[f"yesterday_p{tariff}_kWh"] = (
                    yday.get(f"value_p{tariff}_kWh", None) if yday is not None else None
                )

            self.attributes["yesterday_surplus_kWh"] = (
                yday.get("surplus_kWh", None) if yday is not None else None
            )

            for tariff in (1, 2, 3):
                self.attributes[f"yesterday_surplus_p{tariff}_kWh"] = (
                    yday.get(f"surplus_p{tariff}_kWh", None)
                    if yday is not None
                    else None
                )

            self.attributes["yesterday_hours"] = (
                yday.get("delta_h", None) if yday is not None else None
            )

            month = utils.get_by_key(
                self.data["consumptions_monthly_sum"], "datetime", month_starts
            )
            self.attributes["month_kWh"] = (
                month.get("value_kWh", None) if month is not None else None
            )
            self.attributes["month_surplus_kWh"] = (
                month.get("surplus_kWh", None) if month is not None else None
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

            for tariff in (1, 2, 3):
                self.attributes[f"month_p{tariff}_kWh"] = (
                    month.get(f"value_p{tariff}_kWh", None)
                    if month is not None
                    else None
                )
                self.attributes[f"month_surplus_p{tariff}_kWh"] = (
                    month.get(f"surplus_p{tariff}_kWh", None)
                    if month is not None
                    else None
                )

            last_month = utils.get_by_key(
                self.data["consumptions_monthly_sum"],
                "datetime",
                (month_starts - relativedelta(months=1)),
            )
            self.attributes["last_month_kWh"] = (
                last_month.get("value_kWh", None) if last_month is not None else None
            )
            self.attributes["last_month_surplus_kWh"] = (
                last_month.get("surplus_kWh", None) if last_month is not None else None
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
            for tariff in (1, 2, 3):
                self.attributes[f"last_month_p{tariff}_kWh"] = (
                    last_month.get(f"value_p{tariff}_kWh", None)
                    if last_month is not None
                    else None
                )
                self.attributes[f"last_month_surplus_p{tariff}_kWh"] = (
                    last_month.get(f"surplus_p{tariff}_kWh", None)
                    if last_month is not None
                    else None
                )

            if len(self.data["consumptions"]) > 0:
                self.attributes["last_registered_date"] = self.data["consumptions"][-1][
                    "datetime"
                ]

                if len(self.data["consumptions_daily_sum"]) > 0:
                    last_day = self.data["consumptions_daily_sum"][-1]
                    self.attributes["last_registered_day_kWh"] = (
                        last_day.get("value_kWh", None)
                        if last_day is not None
                        else None
                    )
                    self.attributes["last_registered_day_surplus_kWh"] = (
                        last_day.get("surplus_kWh", None)
                        if last_day is not None
                        else None
                    )

                    for tariff in (1, 2, 3):
                        self.attributes[f"last_registered_day_p{tariff}_kWh"] = (
                            last_day.get(f"value_p{tariff}_kWh", None)
                            if last_day is not None
                            else None
                        )

                        self.attributes[
                            f"last_registered_day_surplus_p{tariff}_kWh"
                        ] = (
                            last_day.get(f"surplus_p{tariff}_kWh", None)
                            if last_day is not None
                            else None
                        )

                    self.attributes["last_registered_day_hours"] = (
                        last_day.get("delta_h", None) if last_day is not None else None
                    )

    def process_maximeter(self):
        """Process maximeter data."""
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
        """Process costs."""
        if self.enable_billing:
            new_data_from = self._date_from
            if self._incremental_update:
                with contextlib.suppress(Exception):
                    new_data_from = self.data["cost_monthly_sum"][-1]["datetime"]

            proc = BillingProcessor(
                BillingInput(
                    contracts=self.data["contracts"],
                    consumptions=[
                        x
                        for x in self.data["consumptions"]
                        if x["datetime"] >= new_data_from
                    ],
                    prices=(
                        [x for x in self.data["pvpc"] if x["datetime"] >= new_data_from]
                        if self.is_pvpc
                        else None
                    ),
                    rules=self.pricing_rules,
                )
            )
            month_starts = datetime(
                datetime.today().year, datetime.today().month, 1, 0, 0, 0
            )

            # append new data
            hourly = proc.output["hourly"]
            self.data["cost_hourly_sum"] = utils.extend_and_filter(
                self.data["cost_hourly_sum"],
                hourly,
                "datetime",
                self._date_from,
                self._date_to,
            )

            daily = proc.output["daily"]
            self.data["cost_daily_sum"] = utils.extend_and_filter(
                self.data["cost_daily_sum"],
                daily,
                "datetime",
                self._date_from,
                self._date_to,
            )

            monthly = proc.output["monthly"]
            self.data["cost_monthly_sum"] = utils.extend_and_filter(
                self.data["cost_monthly_sum"],
                monthly,
                "datetime",
                self._date_from,
                self._date_to,
            )

            this_month = utils.get_by_key(
                self.data["cost_monthly_sum"],
                "datetime",
                month_starts,
            )

            last_month = utils.get_by_key(
                self.data["cost_monthly_sum"],
                "datetime",
                (month_starts - relativedelta(months=1)),
            )

            if this_month is not None:
                self.attributes["month_€"] = this_month.get("value_eur", None)

            if last_month is not None:
                self.attributes["last_month_€"] = last_month.get("value_eur", None)

    def reset(self):
        """Reset in-mem objects."""

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

        for attr in ATTRIBUTES:
            self.attributes[attr] = None

        self.last_update = {x: datetime(1970, 1, 1) for x in self.data}
