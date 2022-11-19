"""Tests for DatadisConnector (offline)"""

import datetime
from unittest.mock import MagicMock, patch

import pytest

from ..connectors.datadis import DatadisConnector

MOCK_USERNAME = "USERNAME"
MOCK_PASSWORD = "PASSWORD"

SUPPLIES_RESPONSE = [
    {
        "cups": "ESXXXXXXXXXXXXXXXXTEST",
        "validDateFrom": "2022/03/09",
        "validDateTo": "2022/10/28",
        "address": "-",
        "postalCode": "-",
        "province": "-",
        "municipality": "-",
        "distributor": "-",
        "pointType": 5,
        "distributorCode": "2",
    }
]
SUPPLIES_EXPECTATIONS = [
    {
        "cups": "ESXXXXXXXXXXXXXXXXTEST",
        "date_start": datetime.datetime(2022, 3, 9, 0, 0),
        "date_end": datetime.datetime(2022, 10, 28, 0, 0),
        "address": "-",
        "postal_code": "-",
        "province": "-",
        "municipality": "-",
        "distributor": "-",
        "pointType": 5,
        "distributorCode": "2",
    }
]

CONTRACTS_RESPONSE = [
    {
        "startDate": "2022/03/09",
        "endDate": "2022/10/28",
        "marketer": "MARKETER",
        "distributorCode": "2",
        "contractedPowerkW": [4.4, 4.4],
    }
]

CONTRACTS_EXPECTATIONS = [
    {
        "date_start": datetime.datetime(2022, 3, 9, 0, 0),
        "date_end": datetime.datetime(2022, 10, 28, 0, 0),
        "marketer": "MARKETER",
        "distributorCode": "2",
        "power_p1": 4.4,
        "power_p2": 4.4,
    }
]

CONSUMPTIONS_RESPONSE = [
    {
        "date": "2022/10/22",
        "time": "01:00",
        "consumptionKWh": 0.203,
        "obtainMethod": "Real",
    },
    {
        "date": "2022/10/22",
        "time": "02:00",
        "consumptionKWh": 0.163,
        "obtainMethod": "Real",
    },
]

CONSUMPTIONS_EXPECTATIONS = [
    {
        "datetime": datetime.datetime(2022, 10, 22, 0, 0, 0),
        "delta_h": 1,
        "value_kWh": 0.203,
        "real": True,
    },
    {
        "datetime": datetime.datetime(2022, 10, 22, 1, 0, 0),
        "delta_h": 1,
        "value_kWh": 0.163,
        "real": True,
    },
]


MAXIMETER_RESPONSE = [
    {
        "date": "2022/03/10",
        "time": "14:15",
        "maxPower": 2.436,
    },
    {
        "date": "2022/03/14",
        "time": "13:15",
        "maxPower": 3.008,
    },
    {
        "date": "2022/03/27",
        "time": "10:30",
        "maxPower": 3.288,
    },
]

MAXIMETER_EXPECTATIONS = [
    {"datetime": datetime.datetime(2022, 3, 10, 14, 15, 0), "value_kW": 2.436},
    {"datetime": datetime.datetime(2022, 3, 14, 13, 15, 0), "value_kW": 3.008},
    {"datetime": datetime.datetime(2022, 3, 27, 10, 30, 0), "value_kW": 3.288},
]


@pytest.mark.order(1)
@patch.object(DatadisConnector, "_get_token", MagicMock(return_value=True))
@patch.object(DatadisConnector, "_send_cmd", MagicMock(return_value=SUPPLIES_RESPONSE))
def test_get_supplies():
    """Test a successful 'get_supplies' query"""
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert SUPPLIES_EXPECTATIONS == connector.get_supplies()


@pytest.mark.order(2)
@patch.object(DatadisConnector, "_get_token", MagicMock(return_value=True))
@patch.object(DatadisConnector, "_send_cmd", MagicMock(return_value=CONTRACTS_RESPONSE))
def test_get_contract_detail():
    """Test a successful 'get_contract_detail' query"""
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert CONTRACTS_EXPECTATIONS == connector.get_contract_detail(
        "ESXXXXXXXXXXXXXXXXTEST", "2"
    )


@pytest.mark.order(3)
@patch.object(DatadisConnector, "_get_token", MagicMock(return_value=True))
@patch.object(
    DatadisConnector, "_send_cmd", MagicMock(return_value=CONSUMPTIONS_RESPONSE)
)
def test_get_consumption_data():
    """Test a successful 'get_consumption_data' query"""
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert CONSUMPTIONS_EXPECTATIONS == connector.get_consumption_data(
        "ESXXXXXXXXXXXXXXXXTEST",
        "2",
        datetime.datetime(2022, 10, 22, 0, 0, 0),
        datetime.datetime(2022, 10, 22, 2, 0, 0),
        0,
        5,
    )


@pytest.mark.order(4)
@patch.object(DatadisConnector, "_get_token", MagicMock(return_value=True))
@patch.object(DatadisConnector, "_send_cmd", MagicMock(return_value=MAXIMETER_RESPONSE))
def test_get_max_power():
    """Test a successful 'get_max_power' query"""
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert MAXIMETER_EXPECTATIONS == connector.get_max_power(
        "ESXXXXXXXXXXXXXXXXTEST",
        "2",
        datetime.datetime(2022, 3, 1, 0, 0, 0),
        datetime.datetime(2022, 4, 1, 0, 0, 0),
        5,
    )
