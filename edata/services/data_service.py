"""Definition of a service for telemetry data handling."""

import asyncio
import logging
import os
import typing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import gettempdir

from dateutil import relativedelta

from edata.core.completion import PendingLedger, is_day_final, is_month_final
from edata.core.utils import (
    get_db_path,
    get_day,
    get_month,
    get_tariff,
    redacted_cups,
)
from edata.database.controller import EdataDB
from edata.database.migrations import MigrationResult, run_migrations
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

        self._storage_path = storage_path
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

    async def get_last_energy_dt(self) -> datetime | None:
        """Return the timestamp of the latest energy record."""

        return await self._get_last_energy_dt()

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

            # fetch upstream records
            await self.update_energy(last_energy_dt + timedelta(hours=1), end_date)
        else:
            # we have no data yet, fetch from start
            await self.update_energy(start_date, end_date)

        # update power records
        await self.update_power(start_date, end_date)

        # fetch pvpc data
        await self.update_pvpc(start_date, end_date)

        # (re)compile only the statistics buckets that are new or still incomplete
        await self.update_statistics_incremental()

        return True

    async def login(self) -> bool:
        """Test login at Datadis."""

        return await self.datadis.async_login()

    async def update_supplies(self) -> None:
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
                cups, supply.distributor_code, self._authorized_nif
            )
            for c in self._contracts:
                await self.db.add_contract(self._cups, c)
            return True
        _LOGGER.warning("Unable to fetch contract details for %s", self._scups)
        return False

    async def update_energy(self, start: datetime, end: datetime) -> bool:
        """Update the list of energy consumptions for the selected cups."""

        cups = self._cups
        supply = self._find_supply_for_cups(cups)
        if supply:
            data = await self.datadis.async_get_consumption_data(
                cups,
                supply.distributor_code,
                start,
                end,
                self._measurement_type,
                supply.point_type,
                self._authorized_nif,
            )
            await self.db.add_energy_list(self._cups, data)
            return True
        _LOGGER.warning("Unable to fetch energy data for %s", self._scups)
        return False

    async def update_power(self, start: datetime, end: datetime) -> bool:
        """Update the list of power peaks for the selected cups."""
        cups = self._cups
        supply = self._find_supply_for_cups(cups)
        if supply:
            data = await self.datadis.async_get_max_power(
                cups,
                supply.distributor_code,
                start,
                end,
                self._authorized_nif,
            )
            await self.db.add_power_list(self._cups, data)
            return True
        _LOGGER.warning("Unable to fetch power data for %s", self._scups)
        return False

    async def update_pvpc(self, start: datetime, end: datetime) -> bool:
        """Update recent pvpc prices."""

        cups = self._cups
        min_date = get_day(datetime.now()) - timedelta(days=28)
        if start < min_date:
            start = min_date
        if end < min_date:
            # end date out of bounds
            return False
        if _pvpc_dt := await self._get_last_pvpc_dt():
            # keep within the fetchable window even if the last stored price is
            # older than a month, otherwise REData rejects the range with a 400
            start = max(_pvpc_dt + timedelta(hours=1), min_date)
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

    async def run_migrations(
        self, compile_statistics: bool = True
    ) -> list[MigrationResult]:
        """Run pending data migrations (e.g. import a legacy 1.3.3 JSON export).

        Migrations import raw records only; when anything is imported and
        ``compile_statistics`` is set, the day/month statistics are compiled from
        the freshly-imported energy so the history is queryable right away. Bills
        are left to the normal billing flow (they need user rules).
        """

        results = await run_migrations(self.db, self._storage_path, self._cups)
        if not results or not compile_statistics:
            return results

        supply = await self.get_supply()
        last_energy_dt = await self._get_last_energy_dt()
        if supply is not None and last_energy_dt is not None:
            await self.update_statistics(supply.date_start, last_energy_dt)
        return results

    async def update_statistics(self, start: datetime, end: datetime) -> None:
        """Compile the statistics for a range plus any incomplete backlog."""

        ledger = PendingLedger()
        ledger.mark_range(start, end)
        await self._seed_pending_backlog(ledger)
        await self._compile_pending(ledger)

    async def update_statistics_incremental(self) -> None:
        """Compile only the statistics buckets that are new or still incomplete.

        The pending buckets are derived from cheap indexed queries (incomplete
        rows plus the recent range past the last complete bucket), so a sync never
        scans the whole energy history.
        """

        ledger = PendingLedger()
        await self._seed_pending_backlog(ledger)
        await self._seed_pending_recent(ledger)
        await self._compile_pending(ledger)

    async def _seed_pending_backlog(self, ledger: PendingLedger) -> None:
        """Seed the ledger with the buckets whose stored stats are incomplete."""

        ledger.add_days(await self._find_missing_stats("day"))
        ledger.add_months(await self._find_missing_stats("month"))

    async def _seed_pending_recent(self, ledger: PendingLedger) -> None:
        """Seed the ledger with the range past the last complete daily stat."""

        last_energy = await self.db.get_last_energy(self._cups)
        if not last_energy:
            return

        last_complete = await self.db.get_last_complete_statistic(self._cups, "day")
        if last_complete:
            start = get_day(last_complete.datetime) + timedelta(days=1)
        else:
            supply = await self.db.get_supply(self._cups)
            start = get_day(
                supply.data.date_start if supply else last_energy.datetime
            )
        ledger.mark_range(start, last_energy.datetime)

    async def _compile_pending(self, ledger: PendingLedger) -> None:
        """Compile the pending buckets, one month of energy loaded at a time."""

        now = datetime.now()
        for month in ledger.sorted_months():
            month_end = (
                month + relativedelta.relativedelta(months=1) - timedelta(microseconds=1)
            )
            data = await self.get_energy(month, month_end)

            for day in ledger.days_in(month):
                day_end = day + timedelta(days=1) - timedelta(microseconds=1)
                day_data = [x for x in data if day <= x.datetime <= day_end]
                stat = await asyncio.to_thread(
                    self._compile_statistics, day_data, get_day
                )
                delta_h = stat[0].delta_h if stat else 0.0
                final = is_day_final(day, delta_h, now)
                if stat:
                    await self.db.add_statistics(self._cups, "day", stat[0], final)
                ledger.resolve_day(day, final=final)

            month_stat = await asyncio.to_thread(
                self._compile_statistics, data, get_month
            )
            delta_h = month_stat[0].delta_h if month_stat else 0.0
            final = is_month_final(month, delta_h, now)
            if month_stat:
                await self.db.add_statistics(self._cups, "month", month_stat[0], final)
            ledger.resolve_month(month, final=final)

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
                    value_kwh=0,
                    consumption_by_tariff=[0.0, 0.0, 0.0],
                    surplus_kwh=0,
                    surplus_by_tariff=[0.0, 0.0, 0.0],
                    generation_kwh=0,
                    generation_by_tariff=[0.0, 0.0, 0.0],
                    selfconsumption_kwh=0,
                    selfconsumption_by_tariff=[0.0, 0.0, 0.0],
                )

            ref = agg_data[agg_dt]
            ref.delta_h += item.delta_h
            ref.consumption_kwh += item.consumption_kwh
            ref.surplus_kwh += item.surplus_kwh
            ref.generation_kwh += item.generation_kwh
            ref.selfconsumption_kwh += item.selfconsumption_kwh
            if 1 <= tariff <= 3:
                idx = tariff - 1
                ref.consumption_by_tariff[idx] += item.consumption_kwh
                ref.surplus_by_tariff[idx] += item.surplus_kwh
                ref.generation_by_tariff[idx] += item.generation_kwh
                ref.selfconsumption_by_tariff[idx] += item.selfconsumption_kwh

        return [agg_data[x] for x in agg_data]

    async def _find_missing_stats(
        self, agg: typing.Literal["day", "month"] = "day"
    ) -> list[datetime]:
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

    async def _sync(self) -> None:
        """Load state."""

        self._supplies = await self.get_supplies()
        self._contracts = await self.get_contracts()
