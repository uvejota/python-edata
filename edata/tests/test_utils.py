"""Tests for edata.core.utils helpers."""

from datetime import datetime, timedelta

import pytest

from edata.core.utils import get_month, iter_month_windows


def _assert_windows_valid(
    windows: list[tuple[datetime, datetime]],
    start: datetime,
    end: datetime,
) -> None:
    """Assert windows tile [start, end] contiguously, one calendar month each."""
    assert windows[0][0] == start
    assert windows[-1][1] == end
    for win_start, win_end in windows:
        assert win_start <= win_end
        # a window never straddles a calendar-month boundary
        assert get_month(win_start) == get_month(win_end)
    for (_, prev_end), (next_start, _) in zip(windows, windows[1:], strict=False):
        # contiguous and non-overlapping
        assert next_start == prev_end + timedelta(microseconds=1)


@pytest.mark.parametrize(
    ("start", "end", "expected_len"),
    [
        # sub-month range collapses to a single window
        (datetime(2023, 1, 5), datetime(2023, 1, 20), 1),
        # spans three calendar months
        (datetime(2023, 1, 15), datetime(2023, 3, 10), 3),
        # exact month boundaries
        (datetime(2023, 1, 1), datetime(2023, 2, 28, 23, 59, 59), 2),
        # crosses a year boundary
        (datetime(2022, 12, 20), datetime(2023, 2, 1), 3),
        # spring-forward DST month is handled like any other (naive datetimes)
        (datetime(2023, 3, 1), datetime(2023, 4, 15), 2),
    ],
    ids=["sub-month", "three-months", "exact-boundaries", "year-boundary", "dst-month"],
)
def test_iter_month_windows_tiles_range(
    start: datetime, end: datetime, expected_len: int
) -> None:
    """Windows cover the range contiguously, one calendar month at a time."""
    windows = list(iter_month_windows(start, end))
    assert len(windows) == expected_len
    _assert_windows_valid(windows, start, end)


def test_iter_month_windows_single_instant() -> None:
    """A zero-length range yields exactly one window covering that instant."""
    moment = datetime(2023, 6, 15, 10, 30)
    assert list(iter_month_windows(moment, moment)) == [(moment, moment)]


def test_iter_month_windows_empty_when_end_before_start() -> None:
    """An inverted range yields no windows."""
    assert list(iter_month_windows(datetime(2023, 2, 1), datetime(2023, 1, 1))) == []
