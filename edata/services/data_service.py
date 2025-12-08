"""Definition of a service for telemetry data handling."""

import asyncio
import calendar
import logging
import os
import typing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import gettempdir

from dateutil import relativedelta

from edata.core.utils import get_db_path, get_day, get_month, get_tariff, redacted_cups
from edata.database.controller import EdataDB
from edata.models import Contract, Energy, Power, Statistics, Supply
from edata.models.bill import EnergyPrice
from edata.providers import DatadisConnector, REDataConnector

_LOGGER = logging.getLogger(__name__)


class DataService:
    "Definition of an energy and power data service based on Datadis."

    def __init__(
        self,
        cups: str,
        datadis_user: str,
        datadis_pwd: str,
        storage_path: str,
        datadis_authorized_nif: str | None = None,
    ) -> None:

        self.datadis = DatadisConnector(
            datadis_user, datadis_pwd, storage_path=storage_path
        )
        self.redata = REDataConnector()

        # params
        self._cups = cups
        self._scups = redacted_cups(cups)
        self._authorized_nif = datadis_authorized_nif
        self._measurement_type = "0"

        if self._authorized_nif and self._authorized_nif == datadis_user:
            _LOGGER.warning(
                "Ignoring authorized NIF parameter since it matches the username"
            )
            self._authorized_nif = None

        self.db = EdataDB(get_db_path(storage_path))

        # data (in-memory cache)
        self._supplies: list[Supply] = []
        self._contracts: list[Contract] = []

    async def get_supplies(self) -> list[Supply]:
        """Return the list of supplies."""

        res = await self.db.list_supplies()
        return [x.data for x in res]

    async def get_supply(self) -> Supply | None:
        res = await self.db.get_supply(self._cups)
        if res:
            return res.data
        return None

    async def get_contracts(self) -> list[Contract]:
        """Return a list of contracts for the selected cups."""

        res = await self.db.list_contracts(self._cups)
        return [x.data for x in res]

    async def get_energy(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> list[Energy]:
        """Return a list of energy records for the selected cups."""

        res = await self.db.list_energy(self._cups, start, end)
        return [x.data for x in res]

    async def get_power(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> list[Power]:
        """Return a list of power records for the selected cups."""

        res = await self.db.list_power(self._cups, start, end)
        return [x.data for x in res]

    async def get_pvpc(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> list[EnergyPrice]:
        """Return a list of pvpc records (energy prices) for the selected cups."""

        res = await self.db.list_pvpc(start, end)
        return [x.data for x in res]

    async def get_statistics(
        self,
        type_: typing.Literal["day", "month"],
        start: datetime | None = None,
        end: datetime | None = None,
        complete: bool | None = None,
    ) -> list[Statistics]:
        """Return a list of statistics records for the selected cups."""

        data = await self.db.list_statistics(self._cups, type_, start, end, complete)
        return [x.data for x in data]

    async def fix_missing_statistics(self):
        """Recompile statistics to fix missing data."""

        missing = await self._find_missing_stats()
        for day in missing:
            _LOGGER.info("%s updating daily statistics for date %s", self._scups, day)
            end = day + relativedelta.relativedelta(day=1) - timedelta(hours=1)
            await self._update_daily_statistics(day, end)

        missing_months = await self._find_missing_stats("month")
        for month in missing_months:
            _LOGGER.info(
                "%s updating monthly statistics for date %s", self._scups, month
            )
            end = month + relativedelta.relativedelta(month=1) - timedelta(hours=1)
            await self._update_monthly_statistics(month, end)

    async def update(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> bool:
        """Update all missing data within optional date ranges."""

        await self._sync()

        cups = self._cups
        scups = self._scups

        # update supplies
        await self.update_supplies()
        if not self._supplies:
            _LOGGER.error(
                "%s unable to retrieve any supplies, this is likely due to credential errors (check previous logs) or temporary Datadis unavailability",
                scups,
            )
            return False

        # find requested cups in supplies
        supply = self._find_supply_for_cups(cups)
        if not supply:
            _LOGGER.error(
                (
                    "%s the selected supply is not found in the provided account, got: %s."
                    "This may be expected if you have just registered this cups since it takes a while to appear,"
                    "otherwise check Datadis website and copypaste the CUPS to avoid type errors"
                ),
                scups,
                [redacted_cups(x.cups) for x in self._supplies],
            )
            return False

        _LOGGER.info(
            "%s the selected supply is available from %s to %s",
            scups,
            supply.date_start,
            supply.date_end,
        )

        if not start_date:
            start_date = supply.date_start
            _LOGGER.debug(
                "%s automatically setting start date as supply date start", self._scups
            )
        if not end_date:
            end_date = datetime.today()
            _LOGGER.debug("%s automatically setting end date as today", self._scups)

        _LOGGER.info(
            "%s data will be updated from %s to %s",
            scups,
            start_date,
            end_date,
        )

        # update contracts to get valid periods
        await self.update_contracts()
        if not self._contracts:
            _LOGGER.warning(
                "%s unable to update contracts, edata will assume that the selected supply has no contractual issues",
                scups,
            )

        # update energy records
        last_energy_dt = await self._get_last_energy_dt()
        if last_energy_dt:
            _LOGGER.info(
                "%s the latest known energy timestamp is %s",
                self._scups,
                last_energy_dt,
            )

            # look for missing dates
            missing_days = await self._find_missing_stats()
            missing_days = [
                x for x in missing_days if x >= start_date and x <= end_date
            ]
            _LOGGER.info(
                "%s the following days are missing energy data %s",
                self._scups,
                missing_days,
            )

            for day in missing_days:
                _LOGGER.info("%s trying to update energy for %s", scups, day)
                await self.update_energy(day, day + timedelta(days=1))

            # and recreate statistics
            await self.fix_missing_statistics()

            # fetch upstream records
            await self.update_energy(last_energy_dt + timedelta(hours=1), end_date)
        else:
            # we have no data yet, fetch from start
            await self.update_energy(start_date, end_date)

        # update power records
        await self.update_power(start_date, end_date)

        # fetch pvpc data
        await self.update_pvpc(start_date, end_date)

        # update new statistics
        await self.update_statistics(start_date, end_date)

        return True

    async def login(self):
        """Test login at Datadis."""

        return await self.datadis.async_login()

    async def update_supplies(self):
        """Update the list of supplies for the configured user."""

        self._supplies = await self.datadis.async_get_supplies(self._authorized_nif)
        for s in self._supplies:
            await self.db.add_supply(s)

    async def update_contracts(self) -> bool:
        """Update the list of contracts for the selected cups."""

        cups = self._cups
        supply = self._find_supply_for_cups(cups)
        if supply:
            self._contracts = await self.datadis.async_get_contract_detail(
                cups, supply.distributorCode, self._authorized_nif
            )
            for c in self._contracts:
                await self.db.add_contract(self._cups, c)
            return True
        _LOGGER.warning("Unable to fetch contract details for %s", self._scups)
        return False

    async def update_energy(self, start: datetime, end: datetime):
        """Update the list of energy consumptions for the selected cups."""

        cups = self._cups
        supply = self._find_supply_for_cups(cups)
        if supply:
            data = await self.datadis.async_get_consumption_data(
                cups,
                supply.distributorCode,
                start,
                end,
                self._measurement_type,
                supply.pointType,
                self._authorized_nif,
            )
            await self.db.add_energy_list(self._cups, data)
            return True
        _LOGGER.warning("Unable to fetch energy data for %s", self._scups)
        return False

    async def update_power(self, start: datetime, end: datetime):
        """Update the list of power peaks for the selected cups."""
        cups = self._cups
        supply = self._find_supply_for_cups(cups)
        if supply:
            data = await self.datadis.async_get_max_power(
                cups,
                supply.distributorCode,
                start,
                end,
                self._authorized_nif,
            )
            await self.db.add_power_list(self._cups, data)
            return True
        _LOGGER.warning("Unable to fetch power data for %s", self._scups)
        return False

    async def update_pvpc(self, start: datetime, end: datetime):
        """Update recent pvpc prices."""

        cups = self._cups
        min_date = get_day(datetime.now()) - timedelta(days=28)
        if start < min_date:
            start = min_date
        if end < min_date:
            # end date out of bounds
            return False
        if _pvpc_dt := await self._get_last_pvpc_dt():
            start = _pvpc_dt + timedelta(hours=1)
            if start >= end:
                _LOGGER.info("%s pvpc prices are already synced", self._scups)
                # data is already synced
                return True
        end = get_day(end) + timedelta(hours=23, minutes=59)
        prices = await self.redata.async_get_realtime_prices(start, end)
        if prices:
            await self.db.add_pvpc_list(prices)
            return True
        _LOGGER.warning("%s unable to fetch pvpc prices", self._scups)
        return False

    async def update_statistics(self, start: datetime, end: datetime):
        """Update the statistics during a period."""

        await self._update_daily_statistics(start, end)
        await self._update_monthly_statistics(start, end)

    async def _update_daily_statistics(self, start: datetime, end: datetime):
        """Update daily statistics within a date range."""

        day_start = get_day(start)
        day_end = end
        daily = await self.db.list_statistics(self._cups, "day", day_start, day_end)

        complete = []
        for stat in daily:
            if stat.complete:
                complete.append(stat.datetime)
                continue

        data = await self.get_energy(day_start, day_end)
        stats = await asyncio.to_thread(
            self._compile_statistics, data, get_day, skip=complete
        )

        for stat in stats:
            is_complete = stat.delta_h == 24
            await self.db.add_statistics(self._cups, "day", stat, is_complete)
            if not is_complete:
                _LOGGER.info(
                    "%s daily statistics for %s are incomplete",
                    self._scups,
                    stat.datetime.date(),
                )

    async def _update_monthly_statistics(self, start: datetime, end: datetime):
        """Update monthly statistics within a date range."""

        month_start = get_month(start)
        month_end = end
        monthly = await self.db.list_statistics(
            self._cups, "month", month_start, month_end
        )

        complete = []
        for stat in monthly:
            if stat.complete:
                complete.append(stat.datetime)
                continue

        data = await self.get_energy(month_start, month_end)
        stats = await asyncio.to_thread(
            self._compile_statistics, data, get_month, skip=complete
        )

        for stat in stats:
            target_hours = (
                calendar.monthrange(stat.datetime.year, stat.datetime.month)[1] * 24
            )
            is_complete = stat.delta_h == target_hours
            await self.db.add_statistics(self._cups, "month", stat, is_complete)
            if not is_complete:
                _LOGGER.info(
                    "%s monthly statistics for %s are incomplete",
                    self._scups,
                    stat.datetime.date(),
                )

    def _compile_statistics(
        self,
        data: list[Energy],
        agg: typing.Callable[[datetime], datetime],
        wanted: list[datetime] | None = None,
        skip: list[datetime] | None = None,
    ) -> list[Statistics]:
        """Return the aggregated energy data."""

        if not wanted:
            wanted = []
        if not skip:
            skip = []

        agg_data = {}

        for item in data:

            agg_dt = agg(item.datetime)
            is_wanted = agg_dt in wanted or agg_dt not in skip
            if not is_wanted:
                continue

            tariff = get_tariff(item.datetime)

            if agg_dt not in agg_data:
                agg_data[agg_dt] = Statistics(
                    datetime=agg_dt,
                    delta_h=0,
                    value_kWh=0,
                    value_p1_kWh=0,
                    value_p2_kWh=0,
                    value_p3_kWh=0,
                    surplus_kWh=0,
                    surplus_p1_kWh=0,
                    surplus_p2_kWh=0,
                    surplus_p3_kWh=0,
                )

            ref = agg_data[agg_dt]
            ref.delta_h += item.delta_h
            ref.value_kWh += item.value_kWh
            ref.surplus_kWh += item.surplus_kWh
            if 1 == tariff:
                ref.value_p1_kWh += item.value_kWh
                ref.surplus_p1_kWh += item.surplus_kWh
            elif 2 == tariff:
                ref.value_p2_kWh += item.value_kWh
                ref.surplus_p2_kWh += item.surplus_kWh
            elif 3 == tariff:
                ref.value_p3_kWh += item.value_kWh
                ref.surplus_p3_kWh += item.surplus_kWh

        return [agg_data[x] for x in agg_data]

    async def _find_missing_stats(self, agg: typing.Literal["day", "month"] = "day"):
        """Return the list of days that are missing energy data."""

        stats = await self.get_statistics(agg, complete=False)
        return [x.datetime for x in stats]

    def _find_supply_for_cups(self, cups: str) -> Supply | None:
        """Return the supply that matches the provided cups."""

        for supply in self._supplies:
            if supply.cups == cups:
                return supply

    async def _get_last_energy_dt(self) -> datetime | None:
        """Return the timestamp of the latest energy record."""

        last_record = await self.db.get_last_energy(self._cups)
        if last_record:
            return last_record.datetime

    async def _get_last_pvpc_dt(self) -> datetime | None:
        """Return the timestamp of the latest pvpc record."""

        last_record = await self.db.get_last_pvpc()
        if last_record:
            return last_record.datetime

    async def _sync(self):
        """Load state."""

        self._supplies = await self.get_supplies()
        self._contracts = await self.get_contracts()
