"""Tests for DatadisConnector (offline)."""

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from edata.providers.datadis import DatadisConnector

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

CONTRACTS_RESPONSE = [
    {
        "startDate": "2022/03/09",
        "endDate": "2022/10/28",
        "marketer": "MARKETER",
        "distributorCode": "2",
        "contractedPowerkW": [4.4, 4.4],
    }
]

CONSUMPTIONS_RESPONSE = [
    {
        "date": "2022/10/22",
        "time": "01:00",
        "consumptionKWh": 0.203,
        "surplusEnergyKWh": 0,
        "obtainMethod": "Real",
    },
    {
        "date": "2022/10/22",
        "time": "02:00",
        "consumptionKWh": 0.163,
        "surplusEnergyKWh": 0,
        "obtainMethod": "Real",
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


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_supplies(mock_token, mock_get, snapshot):
    """Test a successful 'get_supplies' query."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=SUPPLIES_RESPONSE)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_supplies() == snapshot


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_contract_detail(mock_token, mock_get, snapshot):
    """Test a successful 'get_contract_detail' query."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=CONTRACTS_RESPONSE)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_contract_detail("ESXXXXXXXXXXXXXXXXTEST", "2") == snapshot


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_consumption_data(mock_token, mock_get, snapshot):
    """Test a successful 'get_consumption_data' query."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=CONSUMPTIONS_RESPONSE)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert (
        connector.get_consumption_data(
            "ESXXXXXXXXXXXXXXXXTEST",
            "2",
            datetime.datetime(2022, 10, 22, 0, 0, 0),
            datetime.datetime(2022, 10, 22, 2, 0, 0),
            "0",
            5,
        )
        == snapshot
    )


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_max_power(mock_token, mock_get, snapshot):
    """Test a successful 'get_max_power' query."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=MAXIMETER_RESPONSE)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert (
        connector.get_max_power(
            "ESXXXXXXXXXXXXXXXXTEST",
            "2",
            datetime.datetime(2022, 3, 1, 0, 0, 0),
            datetime.datetime(2022, 4, 1, 0, 0, 0),
            None,
        )
        == snapshot
    )


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_supplies_empty_response(mock_token, mock_get, snapshot):
    """Test get_supplies with empty response."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=[])
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_supplies() == snapshot


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_supplies_malformed_response(mock_token, mock_get, snapshot):
    """Test get_supplies with malformed response (missing required fields, syrupy snapshot)."""
    malformed = [{"validDateFrom": "2022/03/09"}]  # missing 'cups', etc.
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=malformed)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_supplies() == snapshot


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_supplies_partial_response(mock_token, mock_get, snapshot):
    """Test get_supplies with partial valid/invalid response."""
    partial = [
        SUPPLIES_RESPONSE[0],
        {"validDateFrom": "2022/03/09"},  # invalid
    ]
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=partial)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_supplies() == snapshot


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_consumption_data_cache(mock_token, mock_get, snapshot):
    """Test get_consumption_data uses cache on second call (should not call HTTP again, syrupy snapshot)."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=CONSUMPTIONS_RESPONSE)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    # First call populates cache
    assert (
        connector.get_consumption_data(
            "ESXXXXXXXXXXXXXXXXTEST",
            "2",
            datetime.datetime(2022, 10, 22, 0, 0, 0),
            datetime.datetime(2022, 10, 22, 2, 0, 0),
            "0",
            5,
        )
        == snapshot
    )
    # Second call should use cache, not call HTTP again
    mock_get.reset_mock()
    assert (
        connector.get_consumption_data(
            "ESXXXXXXXXXXXXXXXXTEST",
            "2",
            datetime.datetime(2022, 10, 22, 0, 0, 0),
            datetime.datetime(2022, 10, 22, 2, 0, 0),
            "0",
            5,
        )
        == snapshot
    )
    mock_get.assert_not_called()


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_supplies_optional_fields_none(mock_token, mock_get, snapshot):
    """Test get_supplies with optional fields as None."""
    response = [
        {
            "cups": "ESXXXXXXXXXXXXXXXXTEST",
            "validDateFrom": "2022/03/09",
            "validDateTo": "2022/10/28",
            "address": None,
            "postalCode": None,
            "province": None,
            "municipality": None,
            "distributor": None,
            "pointType": 5,
            "distributorCode": "2",
        }
    ]
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.json = AsyncMock(return_value=response)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_supplies() == snapshot
