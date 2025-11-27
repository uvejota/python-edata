import asyncio
import calendar
import logging
import os
import typing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import gettempdir

from dateutil import relativedelta
from jinja2 import Environment

from edata.core.utils import (
    get_db_path,
    get_contract_for_dt,
    get_day,
    get_month,
    get_tariff,
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

        # fetch and filter energy items
        energy = await self._get_energy()

        _LOGGER.debug("%s compiling missing hourly bills", self._scups)
        if is_pvpc:
            pvpc = await self._get_pvpc()
            billing_rules = PVPCBillingRules(**billing_rules.model_dump())
            bills = await asyncio.to_thread(
                self._compile_pvpc, contracts, energy, pvpc, billing_rules
            )
            confighash = f"pvpc-{hash(billing_rules.model_dump_json())}"
        else:
            bills = await asyncio.to_thread(
                self._compile_j2, contracts, energy, billing_rules
            )
            confighash = f"custom-{hash(billing_rules.model_dump_json())}"

        _LOGGER.debug("%s pushing hourly bills", self._scups)
        await self.db.add_bill_list(
            cups=self._cups,
            type_="hour",
            confhash=confighash,
            complete=True,
            bill=bills,
        )

        _LOGGER.debug("%s updating daily and monthly bills", self._scups)
        await self.update_statistics(start, end)
        await self.fix_missing_statistics()

    async def update_statistics(self, start: datetime, end: datetime):
        """Update the statistics during a period."""

        await self._update_daily_statistics(start, end)
        await self._update_monthly_statistics(start, end)

    async def _update_daily_statistics(self, start: datetime, end: datetime):
        """Update daily statistics within a date range."""

        day_start = get_day(start)
        day_end = end
        daily = await self.db.list_bill(self._cups, "day", day_start, day_end)

        complete = []
        for stat in daily:
            if stat.complete:
                complete.append(stat.datetime)
                continue

        data = await self.get_bills(day_start, day_end)
        stats = await asyncio.to_thread(
            self._compile_statistics, data, get_day, skip=complete
        )

        for stat in stats:
            is_complete = stat.delta_h == 24
            await self.db.add_bill(self._cups, "day", stat, "mix", is_complete)
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
        monthly = await self.db.list_bill(self._cups, "month", month_start, month_end)

        complete = []
        for stat in monthly:
            if stat.complete:
                complete.append(stat.datetime)
                continue

        data = await self.get_bills(month_start, month_end)
        stats = await asyncio.to_thread(
            self._compile_statistics, data, get_month, skip=complete
        )

        for stat in stats:
            target_hours = (
                calendar.monthrange(stat.datetime.year, stat.datetime.month)[1] * 24
            )
            is_complete = stat.delta_h == target_hours
            await self.db.add_bill(self._cups, "month", stat, "mix", is_complete)
            if not is_complete:
                _LOGGER.info(
                    "%s monthly statistics for %s are incomplete",
                    self._scups,
                    stat.datetime.date(),
                )

    async def _find_missing_stats(self):
        """Return the list of days that are missing billing data."""

        stats = await self.db.list_bill(self._cups, "day", complete=False)
        return [x.datetime for x in stats]

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

    async def fix_missing_statistics(self):
        """Recompile statistics to fix missing data."""

        missing = await self._find_missing_stats()
        for day in missing:
            _LOGGER.debug("%s updating daily statistics for date %s", self._scups, day)
            end = day + relativedelta.relativedelta(day=1) - timedelta(minutes=1)
            await self._update_daily_statistics(day, end)

        missing_months = list(set([get_month(x) for x in missing]))
        for month in missing_months:
            _LOGGER.debug(
                "%s updating monthly statistics for date %s", self._scups, month
            )
            end = month + relativedelta.relativedelta(months=1) - timedelta(minutes=1)
            await self._update_monthly_statistics(month, end)

    def _compile_pvpc(
        self,
        contracts: list[Contract],
        energy: list[Energy],
        pvpc: list[EnergyPrice],
        rules: PVPCBillingRules,
    ) -> list[Bill]:
        """Compile bills assuming PVPC billing."""

        energy_dt = [x.datetime for x in energy]
        pvpc_dt = [x.datetime for x in pvpc]

        # reduce computation range to valid periods
        pvpc_valid_dt = []
        for contract in contracts:
            for dt in pvpc_dt:
                if dt in pvpc_valid_dt:
                    continue
                if contract.date_start <= dt <= contract.date_end:
                    pvpc_valid_dt.append(dt)
        pvpc_valid_dt = [x for x in pvpc_valid_dt if x in energy_dt]

        e = {x.datetime: x for x in energy if x.datetime in pvpc_valid_dt}
        p = {x.datetime: x for x in pvpc if x.datetime in pvpc_valid_dt}
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
                * p[dt].value_eur_kWh
                * e[dt].value_kWh
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

    def _compile_j2(
        self,
        contracts: list[Contract],
        energy: list[Energy],
        rules: BillingRules,
    ):
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
            params["kwh"] = e[dt].value_kWh

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

        res = await self.db.list_contracts(self._cups)
        return [x.data for x in res]

    async def _get_energy(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> list[Energy]:
        res = await self.db.list_energy(self._cups, start, end)
        return [x.data for x in res]

    async def _get_pvpc(
        self, start: datetime | None = None, end: datetime | None = None
    ) -> list[EnergyPrice]:
        res = await self.db.list_pvpc(start, end)
        return [x.data for x in res]

    async def _get_last_bill_dt(self) -> datetime | None:
        """Return the timestamp of the latest bill record."""

        last_record = await self.db.get_last_bill(self._cups)
        if last_record:
            return last_record.datetime
