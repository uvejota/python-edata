import asyncio
import logging
import os
import typing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import gettempdir

from dateutil import relativedelta
from jinja2 import Environment

from edata.core.completion import PendingLedger, is_day_final, is_month_final
from edata.core.utils import (
    get_db_path,
    get_contract_for_dt,
    get_day,
    get_month,
    get_tariff,
    iter_month_windows,
    redacted_cups,
)
from edata.database.controller import EdataDB
from edata.models import Contract, Energy, EnergyPrice
from edata.models.bill import Bill, BillingRules, PVPCBillingRules

_LOGGER = logging.getLogger(__name__)


class BillService:
    "Definition of a bill service for energy supplies."

    def __init__(self, cups: str, storage_path: str) -> None:

        self._cups = cups
        self._scups = redacted_cups(cups)

        self.db = EdataDB(get_db_path(storage_path))

    async def get_bills(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        type_: typing.Literal["hour", "day", "month"] = "hour",
    ) -> list[Bill]:
        """Return the list of bills."""

        res = await self.db.list_bill(self._cups, type_, start, end)
        return [x.data for x in res]

    async def update(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        billing_rules: BillingRules | PVPCBillingRules | None = None,
        is_pvpc: bool = True,
    ) -> None:
        """Update all missing billing data within optional date ranges."""

        if not billing_rules:
            # assuming pvpc
            _LOGGER.info("%s non explicit billing rules, assuming PVPC", self._scups)
            billing_rules = PVPCBillingRules()
            is_pvpc = True

        # fetch cups
        supply = await self.db.get_supply(self._cups)

        if not supply:
            _LOGGER.warning(
                "%s the selected cups does not exist, please fetch data first",
                self._scups,
            )
            return

        _LOGGER.info(
            "%s the selected supply is available from %s to %s",
            self._scups,
            supply.data.date_start,
            supply.data.date_end,
        )

        if not start:
            _LOGGER.debug(
                "%s automatically setting start date as last hourly bill", self._scups
            )
            start = await self._get_last_bill_dt()

        if not start:
            _LOGGER.debug(
                "%s there are no bills for this cups, building since the start of the supply",
                self._scups,
            )
            start = supply.data.date_start

        if not end:
            _LOGGER.debug(
                "%s automatically setting end date as supply date end", self._scups
            )
            end = supply.data.date_end

        _LOGGER.info(
            "%s data will be updated from %s to %s",
            self._scups,
            start,
            end,
        )

        # fetch contracts
        contracts = await self._get_contracts()

        if is_pvpc:
            billing_rules = PVPCBillingRules(**billing_rules.model_dump())
            confighash = f"pvpc-{hash(billing_rules.model_dump_json())}"
        else:
            confighash = f"custom-{hash(billing_rules.model_dump_json())}"

        # compile and persist hourly bills one month at a time so the energy
        # (and simulated bills) held in memory stay bounded to a single month
        for win_start, win_end in iter_month_windows(start, end):
            energy = await self._get_energy(win_start, win_end)
            if not energy:
                continue

            _LOGGER.debug(
                "%s compiling hourly bills for %s..%s", self._scups, win_start, win_end
            )
            if is_pvpc:
                pvpc = await self._get_pvpc(win_start, win_end)
                bills = await asyncio.to_thread(
                    self.simulate_pvpc, contracts, energy, pvpc, billing_rules
                )
            else:
                bills = await asyncio.to_thread(
                    self.simulate_custom, contracts, energy, billing_rules
                )

            await self.db.add_bill_list(
                cups=self._cups,
                type_="hour",
                confhash=confighash,
                complete=True,
                bill=bills,
            )

        _LOGGER.debug("%s updating daily and monthly bills", self._scups)
        await self.update_statistics_incremental()

    async def clear_bills(self, since: datetime | None = None) -> None:
        """Delete stored bills for this cups, optionally only from a datetime onwards."""

        await self.db.clear_bills(self._cups, since)

    async def simulate(
        self,
        billing_rules: BillingRules | PVPCBillingRules | None = None,
        is_pvpc: bool = True,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Bill]:
        """Compile hourly bills for a date range without persisting them."""

        if not billing_rules:
            _LOGGER.info("%s non explicit billing rules, assuming PVPC", self._scups)
            billing_rules = PVPCBillingRules()
            is_pvpc = True

        contracts = await self._get_contracts()
        energy = await self._get_energy(start, end)

        if is_pvpc:
            pvpc = await self._get_pvpc(start, end)
            billing_rules = PVPCBillingRules(**billing_rules.model_dump())
            return await asyncio.to_thread(
                self.simulate_pvpc, contracts, energy, pvpc, billing_rules
            )

        return await asyncio.to_thread(
            self.simulate_custom, contracts, energy, billing_rules
        )

    async def update_statistics(self, start: datetime, end: datetime):
        """Compile the daily/monthly bills for a range plus any incomplete backlog."""

        ledger = PendingLedger()
        ledger.mark_range(start, end)
        await self._seed_pending_backlog(ledger)
        await self._compile_pending(ledger)

    async def update_statistics_incremental(self) -> None:
        """Compile only the daily/monthly bills that are new or still incomplete.

        The pending buckets come from cheap indexed queries (incomplete rows plus
        the recent range past the last complete bucket), so a sync never rolls up
        the whole hourly-bill history.
        """

        ledger = PendingLedger()
        await self._seed_pending_backlog(ledger)
        await self._seed_pending_recent(ledger)
        await self._compile_pending(ledger)

    async def _seed_pending_backlog(self, ledger: PendingLedger) -> None:
        """Seed the ledger with the buckets whose stored bills are incomplete."""

        day_incomplete = await self.db.list_bill(self._cups, "day", complete=False)
        month_incomplete = await self.db.list_bill(self._cups, "month", complete=False)
        ledger.add_days(x.datetime for x in day_incomplete)
        ledger.add_months(x.datetime for x in month_incomplete)

    async def _seed_pending_recent(self, ledger: PendingLedger) -> None:
        """Seed the ledger with the range past the last complete daily bill."""

        last_bill = await self.db.get_last_bill(self._cups)
        if not last_bill:
            return

        last_complete = await self.db.get_last_complete_bill(self._cups, "day")
        if last_complete:
            start = get_day(last_complete.datetime) + timedelta(days=1)
        else:
            supply = await self.db.get_supply(self._cups)
            start = get_day(
                supply.data.date_start if supply else last_bill.datetime
            )
        ledger.mark_range(start, last_bill.datetime)

    async def _compile_pending(self, ledger: PendingLedger) -> None:
        """Roll up the pending buckets, one month of hourly bills loaded at a time."""

        now = datetime.now()
        for month in ledger.sorted_months():
            month_end = (
                month + relativedelta.relativedelta(months=1) - timedelta(microseconds=1)
            )
            data = await self.get_bills(month, month_end, "hour")

            for day in ledger.days_in(month):
                day_end = day + timedelta(days=1) - timedelta(microseconds=1)
                day_data = [x for x in data if day <= x.datetime <= day_end]
                stat = await asyncio.to_thread(
                    self._compile_statistics, day_data, get_day
                )
                delta_h = stat[0].delta_h if stat else 0.0
                final = is_day_final(day, delta_h, now)
                if stat:
                    await self.db.add_bill(self._cups, "day", stat[0], "mix", final)
                ledger.resolve_day(day, final=final)

            month_stat = await asyncio.to_thread(
                self._compile_statistics, data, get_month
            )
            delta_h = month_stat[0].delta_h if month_stat else 0.0
            final = is_month_final(month, delta_h, now)
            if month_stat:
                await self.db.add_bill(self._cups, "month", month_stat[0], "mix", final)
            ledger.resolve_month(month, final=final)

    def _compile_statistics(
        self,
        data: list[Bill],
        agg: typing.Callable[[datetime], datetime],
        wanted: list[datetime] | None = None,
        skip: list[datetime] | None = None,
    ) -> list[Bill]:
        """Return the aggregated bill data."""

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

            if agg_dt not in agg_data:
                agg_data[agg_dt] = Bill(
                    datetime=agg_dt,
                    delta_h=0,
                )

            ref = agg_data[agg_dt]
            ref.delta_h += item.delta_h
            ref.value_eur += item.value_eur
            ref.energy_term += item.energy_term
            ref.power_term += item.power_term
            ref.others_term += item.others_term
            ref.surplus_term += item.surplus_term

        return [agg_data[x] for x in agg_data]

    def simulate_pvpc(
        self,
        contracts: list[Contract],
        energy: list[Energy],
        pvpc: list[EnergyPrice],
        rules: PVPCBillingRules,
    ) -> list[Bill]:
        """Compile bills assuming PVPC billing."""

        # reduce computation to timestamps present in both series and covered
        # by a contract (set membership instead of nested list scans)
        common_dt = {x.datetime for x in energy} & {x.datetime for x in pvpc}
        valid_dt = {
            dt
            for dt in common_dt
            if any(c.date_start <= dt <= c.date_end for c in contracts)
        }

        e = {x.datetime: x for x in energy if x.datetime in valid_dt}
        p = {x.datetime: x for x in pvpc if x.datetime in valid_dt}
        b: dict[datetime, Bill] = {}

        for dt in e.keys():

            c = get_contract_for_dt(contracts, dt)
            if not c:
                continue

            p1_kw = c.power_p1
            p2_kw = c.power_p2
            if not p1_kw or not p2_kw:
                continue

            bill = Bill(datetime=dt, delta_h=1)

            bill.energy_term = (
                rules.electricity_tax
                * rules.iva_tax
                * p[dt].value_eur_kwh
                * e[dt].consumption_kwh
            )
            bill.power_term = (
                rules.electricity_tax
                * rules.iva_tax
                * (
                    p1_kw * (rules.p1_kw_year_eur + rules.market_kw_year_eur)
                    + p2_kw * rules.p2_kw_year_eur
                )
                / 365
                / 24
            )
            bill.others_term = rules.iva_tax * rules.meter_month_eur / 30 / 24
            bill.value_eur = bill.energy_term + bill.power_term + bill.others_term

            b[dt] = bill

        return [x for x in b.values()]

    def simulate_custom(
        self,
        contracts: list[Contract],
        energy: list[Energy],
        rules: BillingRules,
    ) -> list[Bill]:
        """Compile bills from custom rules."""

        e = {x.datetime: x for x in energy}
        b: dict[datetime, Bill] = {}

        env = Environment()
        energy_expr = env.compile_expression(f"({rules.energy_formula})|float")
        power_expr = env.compile_expression(f"({rules.power_formula})|float")
        others_expr = env.compile_expression(f"({rules.others_formula})|float")

        for dt in e.keys():

            c = get_contract_for_dt(contracts, dt)
            if not c:
                continue

            p1_kw = c.power_p1
            p2_kw = c.power_p2
            if not p1_kw or not p2_kw:
                continue

            bill = Bill(datetime=dt, delta_h=1)

            params = rules.model_dump()
            params["p1_kw"] = p1_kw
            params["p2_kw"] = p2_kw
            params["kwh"] = e[dt].consumption_kwh

            tariff = get_tariff(dt)
            if tariff == 1:
                params["kwh_eur"] = rules.p1_kwh_eur
            elif tariff == 2:
                params["kwh_eur"] = rules.p2_kwh_eur
            elif tariff == 3:
                params["kwh_eur"] = rules.p3_kwh_eur

            energy_term = energy_expr(**params)
            power_term = power_expr(**params)
            others_term = others_expr(**params)

            if energy_term:
                bill.energy_term = round(energy_term, 6)

            if power_term:
                bill.power_term = round(power_term, 6)

            if others_term:
                bill.others_term = round(others_term, 6)

            bill.value_eur = bill.energy_term + bill.power_term + bill.others_term

            b[dt] = bill

        return [x for x in b.values()]

    async def _get_contracts(self) -> list[Contract]:
        """Get contracts."""
        res = await self.db.list_contracts(self._cups)
        return [x.data for x in res]

    async def _get_energy(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> list[Energy]:
        """Get energy."""
        res = await self.db.list_energy(self._cups, start, end)
        return [x.data for x in res]

    async def _get_pvpc(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> list[EnergyPrice]:
        """Get PVPC."""
        res = await self.db.list_pvpc(start, end)
        return [x.data for x in res]

    async def _get_last_bill_dt(self) -> datetime | None:
        """Return the timestamp of the latest bill record."""
        last_record = await self.db.get_last_bill(self._cups)
        if last_record:
            return last_record.datetime
