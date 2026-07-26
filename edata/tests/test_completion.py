"""Tests for the incremental completion ledger."""

from datetime import datetime

import pytest

from edata.core.completion import (
    PendingLedger,
    day_buckets,
    is_day_final,
    is_month_final,
)

NOW = datetime(2026, 7, 26, 12, 0)


def test_day_buckets_covers_range_inclusively() -> None:
    """Every day-start in the range is yielded once."""
    buckets = list(day_buckets(datetime(2023, 1, 30, 5), datetime(2023, 2, 2)))
    assert buckets == [
        datetime(2023, 1, 30),
        datetime(2023, 1, 31),
        datetime(2023, 2, 1),
        datetime(2023, 2, 2),
    ]


@pytest.mark.parametrize(
    ("day", "delta_h", "expected"),
    [
        (datetime(2026, 7, 25), 24, True),  # nominal reached
        (datetime(2026, 7, 25), 25, True),  # fall-back DST day (25h)
        (datetime(2026, 7, 25), 23, False),  # recent + short -> keep
        (datetime(2026, 5, 1), 23, True),  # short but settled (>30d old)
    ],
    ids=["nominal", "dst-25h", "recent-short", "settled-short"],
)
def test_is_day_final(day: datetime, delta_h: float, expected: bool) -> None:
    """A day is final at nominal hours or once settled past the window."""
    assert is_day_final(day, delta_h, NOW) is expected


@pytest.mark.parametrize(
    ("month", "delta_h", "expected"),
    [
        (datetime(2026, 7, 1), 31 * 24, True),  # nominal (July has 31 days)
        (datetime(2026, 7, 1), 700, False),  # current month, short -> keep
        (datetime(2026, 5, 1), 700, True),  # short but settled
    ],
    ids=["nominal", "current-short", "settled-short"],
)
def test_is_month_final(month: datetime, delta_h: float, expected: bool) -> None:
    """A month is final at nominal hours or once settled past the window."""
    assert is_month_final(month, delta_h, NOW) is expected


def test_mark_range_adds_days_and_their_months() -> None:
    """mark_range records each day and the month it belongs to."""
    ledger = PendingLedger()
    ledger.mark_range(datetime(2023, 1, 30), datetime(2023, 2, 1))

    assert ledger.days == {
        datetime(2023, 1, 30),
        datetime(2023, 1, 31),
        datetime(2023, 2, 1),
    }
    assert ledger.months == {datetime(2023, 1, 1), datetime(2023, 2, 1)}


def test_add_days_and_months_normalize_and_pair() -> None:
    """add_days aligns to day-start and also marks the month; add_months aligns."""
    ledger = PendingLedger()
    ledger.add_days([datetime(2023, 3, 15, 8, 30)])
    ledger.add_months([datetime(2023, 4, 20, 0, 0)])

    assert ledger.days == {datetime(2023, 3, 15)}
    assert ledger.months == {datetime(2023, 3, 1), datetime(2023, 4, 1)}


def test_days_in_filters_and_sorts_by_month() -> None:
    """days_in returns only the sorted days belonging to the given month."""
    ledger = PendingLedger()
    ledger.add_days(
        [datetime(2023, 1, 31), datetime(2023, 1, 5), datetime(2023, 2, 3)]
    )
    assert ledger.days_in(datetime(2023, 1, 1)) == [
        datetime(2023, 1, 5),
        datetime(2023, 1, 31),
    ]


def test_resolve_only_drops_when_final() -> None:
    """resolve_* removes a bucket when final and keeps it otherwise."""
    ledger = PendingLedger()
    ledger.mark_range(datetime(2023, 1, 1), datetime(2023, 1, 1))

    ledger.resolve_day(datetime(2023, 1, 1), final=False)
    assert datetime(2023, 1, 1) in ledger.days

    ledger.resolve_day(datetime(2023, 1, 1), final=True)
    assert datetime(2023, 1, 1) not in ledger.days

    ledger.resolve_month(datetime(2023, 1, 1), final=True)
    assert datetime(2023, 1, 1) not in ledger.months
