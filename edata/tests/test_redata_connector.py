"""Tests for REData (online)"""

from datetime import datetime, timedelta

import pytest

from ..connectors.redata import REDataConnector


@pytest.mark.order(5)
def test_get_realtime_prices():
    """Test a successful 'get_realtime_prices' query"""
    connector = REDataConnector()
    yesterday = datetime.now().replace(hour=0, minute=0, second=0) - timedelta(days=1)
    response = connector.get_realtime_prices(
        yesterday, yesterday + timedelta(days=1) - timedelta(minutes=1), False
    )
    assert len(response) == 24
