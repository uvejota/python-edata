"""Tests for DatadisConnector (offline)."""

import datetime
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from edata.providers.datadis import DatadisConnector, earliest_query_start


@pytest.fixture(autouse=True)
def _fixture_data_within_history():
    """Keep the 2022 fixture data inside Datadis's two-year query window."""
    with patch(
        "edata.providers.datadis.earliest_query_start",
        return_value=datetime.datetime(2020, 1, 1),
    ):
        yield


def _json_body(payload) -> AsyncMock:
    """Mock ``reply.read()`` returning ``payload`` as a JSON body."""
    return AsyncMock(return_value=json.dumps(payload).encode())


MOCK_USERNAME = "USERNAME"
MOCK_PASSWORD = "PASSWORD"


def test_connector_uses_namespaced_cache_dir(tmp_path) -> None:
    """A storage_path connector creates the namespaced 'edata_cache' dir."""
    connector = DatadisConnector(
        MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path)
    )
    assert os.path.basename(connector._recent_cache_dir) == "edata_cache"
    assert (tmp_path / "edata_cache").is_dir()

SUPPLIES_RESPONSE = {
    "supplies": [
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
}

CONTRACTS_RESPONSE = {
    "contract": [
        {
            "startDate": "2022/03/09",
            "endDate": "2022/10/28",
            "marketer": "MARKETER",
            "distributorCode": "2",
            "contractedPowerkW": [4.4, 4.4],
        }
    ]
}

CONSUMPTIONS_RESPONSE = {
    "timeCurve": [
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
}

CONSUMPTIONS_RESPONSE_WITH_ZERO_HOUR = {
    "timeCurve": [
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
        # Sporadic i-DE glitch: an extra "00:00" row on a day that already
        # carries its full 24 hours. Must be dropped, not remapped to 23:00.
        {
            "date": "2022/10/22",
            "time": "00:00",
            "consumptionKWh": 0.999,
            "surplusEnergyKWh": 0,
            "obtainMethod": "Real",
        },
    ]
}

MAXIMETER_RESPONSE = {
    "maxPower": [
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
}


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_supplies(mock_token, mock_get, snapshot):
    """Test a successful 'get_supplies' query."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.read = _json_body(SUPPLIES_RESPONSE)
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
    mock_response.read = _json_body(CONTRACTS_RESPONSE)
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
    mock_response.read = _json_body(CONSUMPTIONS_RESPONSE)
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
def test_get_consumption_data_skips_zero_hour(mock_token, mock_get):
    """A stray "00:00" row is dropped, not remapped, and never aborts the fetch."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.read = _json_body(CONSUMPTIONS_RESPONSE_WITH_ZERO_HOUR)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)

    # Range spans the previous day, so a remapped "00:00" -> 2022/10/21 23:00
    # would fall inside it; its absence proves the explicit skip, not the filter.
    result = connector.get_consumption_data(
        "ESXXXXXXXXXXXXXXXXTEST",
        "2",
        datetime.datetime(2022, 10, 21, 0, 0, 0),
        datetime.datetime(2022, 10, 22, 23, 59, 59),
        "0",
        5,
    )

    datetimes = [x.datetime for x in result]
    assert datetimes == [
        datetime.datetime(2022, 10, 22, 0, 0),
        datetime.datetime(2022, 10, 22, 1, 0),
    ]
    assert datetime.datetime(2022, 10, 21, 23, 0) not in datetimes


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_max_power(mock_token, mock_get, snapshot):
    """Test a successful 'get_max_power' query."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.read = _json_body(MAXIMETER_RESPONSE)
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
    mock_response.read = _json_body({"supplies": []})
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_supplies() == snapshot


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_supplies_malformed_response(mock_token, mock_get, snapshot):
    """Test get_supplies with malformed response (missing required fields, syrupy snapshot)."""
    malformed = {"supplies": [{"validDateFrom": "2022/03/09"}]}  # missing 'cups', etc.
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.read = _json_body(malformed)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_supplies() == snapshot


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_get_supplies_partial_response(mock_token, mock_get, snapshot):
    """Test get_supplies with partial valid/invalid response."""
    partial = {"supplies": [
        SUPPLIES_RESPONSE["supplies"][0],
        {"validDateFrom": "2022/03/09"},  # invalid
    ]}
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.read = _json_body(partial)
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
    mock_response.read = _json_body(CONSUMPTIONS_RESPONSE)
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
    response = {
        "supplies": [
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
    }
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="text")
    mock_response.read = _json_body(response)
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD)
    assert connector.get_supplies() == snapshot


@pytest.mark.asyncio
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
async def test_shared_session_is_reused(mock_token, tmp_path, snapshot):
    """A caller-provided session serves the requests and is left open."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read = _json_body(SUPPLIES_RESPONSE)
    session = MagicMock()
    session.get.return_value.__aenter__.return_value = mock_response
    session.close = AsyncMock()

    connector = DatadisConnector(
        MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path), session=session
    )
    supplies = await connector.async_get_supplies()

    assert supplies == snapshot
    session.get.assert_called_once()
    session.close.assert_not_awaited()


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        # 2024-10-01 is already more than two years before the 9th
        (datetime.datetime(2026, 10, 9, 18, 0), datetime.datetime(2024, 11, 1)),
        (datetime.datetime(2026, 10, 1, 0, 0), datetime.datetime(2024, 10, 1)),
        (datetime.datetime(2026, 3, 1, 0, 0, 1), datetime.datetime(2024, 4, 1)),
    ],
)
def test_earliest_query_start(now, expected):
    """The oldest accepted start is the first month fully within two years."""
    assert earliest_query_start(now) == expected


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_queries_are_clamped_to_datadis_history(mock_token, mock_get, tmp_path):
    """Older start dates are moved to the oldest month Datadis accepts."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read = _json_body({"timeCurve": [], "maxPower": []})
    mock_get.return_value.__aenter__.return_value = mock_response
    connector = DatadisConnector(
        MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path)
    )

    with patch(
        "edata.providers.datadis.earliest_query_start",
        return_value=datetime.datetime(2022, 10, 1),
    ):
        connector.get_consumption_data(
            "ESXXXXXXXXXXXXXXXXTEST",
            "2",
            datetime.datetime(2019, 1, 1),
            datetime.datetime(2022, 10, 31),
            "0",
            5,
        )
        connector.get_max_power(
            "ESXXXXXXXXXXXXXXXXTEST",
            "2",
            datetime.datetime(2019, 1, 1),
            datetime.datetime(2022, 10, 31),
        )

    urls = [call.args[0] for call in mock_get.call_args_list]
    assert len(urls) == 2
    assert all("startDate=2022/10&" in url for url in urls)


@patch("aiohttp.ClientSession.get")
@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_queries_entirely_before_history_are_skipped(mock_token, mock_get, tmp_path):
    """A range Datadis no longer serves is not requested at all."""
    connector = DatadisConnector(
        MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path)
    )

    with patch(
        "edata.providers.datadis.earliest_query_start",
        return_value=datetime.datetime(2022, 10, 1),
    ):
        assert (
            connector.get_consumption_data(
                "ESXXXXXXXXXXXXXXXXTEST",
                "2",
                datetime.datetime(2019, 1, 1),
                datetime.datetime(2019, 12, 31),
                "0",
                5,
            )
            == []
        )
        assert (
            connector.get_max_power(
                "ESXXXXXXXXXXXXXXXXTEST",
                "2",
                datetime.datetime(2019, 1, 1),
                datetime.datetime(2019, 12, 31),
            )
            == []
        )

    mock_get.assert_not_called()
