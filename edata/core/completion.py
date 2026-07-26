"""Incremental completion tracking for statistics/bill compilation.

The durable source of truth is the per-bucket ``complete`` flag stored on the
statistics/bill rows. A ``PendingLedger`` is the cheap, in-memory work set of the
day/month buckets a single sync still needs to (re)compile -- seeded from the
incomplete rows plus the small recent range past the last complete bucket -- so a
sync never has to scan the whole energy/bill history.

A bucket is *final* once it reaches its nominal hour count, or once its period has
fully elapsed and is older than ``SETTLE_DAYS`` (so DST days, partial months, and
hours Datadis never delivered leave the ledger instead of being recompiled
forever).
"""

import calendar
import typing
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from edata.core.utils import get_day, get_month

# Past periods older than this settle window are treated as final regardless of
# their hour count. Matches the ~monthly window Datadis uses to revise data.
SETTLE_DAYS = 30

HOURS_PER_DAY = 24


def day_buckets(start: datetime, end: datetime) -> typing.Iterator[datetime]:
    """Yield each day-start in ``[start, end]``."""

    day = get_day(start)
    while day <= end:
        yield day
        day += timedelta(days=1)


def _elapsed_beyond_settle(period_end: datetime, now: datetime, settle_days: int) -> bool:
    """Return whether a period ended more than ``settle_days`` before ``now``."""

    return period_end < now - timedelta(days=settle_days)


def is_day_final(
    day: datetime, delta_h: float, now: datetime, settle_days: int = SETTLE_DAYS
) -> bool:
    """Return whether a daily bucket needs no further recompilation."""

    if delta_h >= HOURS_PER_DAY:
        return True
    return _elapsed_beyond_settle(day + timedelta(days=1), now, settle_days)


def is_month_final(
    month: datetime, delta_h: float, now: datetime, settle_days: int = SETTLE_DAYS
) -> bool:
    """Return whether a monthly bucket needs no further recompilation."""

    nominal = calendar.monthrange(month.year, month.month)[1] * HOURS_PER_DAY
    if delta_h >= nominal:
        return True
    return _elapsed_beyond_settle(month + relativedelta(months=1), now, settle_days)


class PendingLedger:
    """In-memory set of day/month buckets still needing (re)compilation."""

    def __init__(self) -> None:
        self.days: set[datetime] = set()
        self.months: set[datetime] = set()

    def mark_range(self, start: datetime, end: datetime) -> None:
        """Mark every day (and its month) spanning ``[start, end]`` as pending."""

        for day in day_buckets(start, end):
            self.days.add(day)
            self.months.add(get_month(day))

    def add_days(self, days: typing.Iterable[datetime]) -> None:
        """Mark specific days (and their months) as pending."""

        for value in days:
            day = get_day(value)
            self.days.add(day)
            self.months.add(get_month(day))

    def add_months(self, months: typing.Iterable[datetime]) -> None:
        """Mark specific months as pending."""

        for value in months:
            self.months.add(get_month(value))

    def days_in(self, month: datetime) -> list[datetime]:
        """Return the sorted pending days that belong to ``month``."""

        return sorted(day for day in self.days if get_month(day) == month)

    def sorted_months(self) -> list[datetime]:
        """Return the pending months in chronological order."""

        return sorted(self.months)

    def resolve_day(self, day: datetime, *, final: bool) -> None:
        """Drop a day from the ledger once it is final."""

        if final:
            self.days.discard(day)

    def resolve_month(self, month: datetime, *, final: bool) -> None:
        """Drop a month from the ledger once it is final."""

        if final:
            self.months.discard(month)
