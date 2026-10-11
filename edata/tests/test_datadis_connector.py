"""Tests for DatadisConnector (offline)."""

import datetime
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from edata.providers.datadis import DatadisConnector, LoginStatus, earliest_query_start


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


def _first_month_broken_get(bad_month: str, calls: list[tuple[str, str]]):
    """Fake Datadis that fails (500) any query including ``bad_month``.

    Healthy months answer one consumption and one max power row per day 1..3,
    so the merged result can be checked for order and gaps.
    """

    def fake_get(url, **kwargs):
        query = dict(p.split("=") for p in url.split("?")[1].strip("&").split("&"))
        start, end = query["startDate"], query["endDate"]
        calls.append((start, end))
        reply = MagicMock()
        reply.text = AsyncMock(return_value="Internal Server Error")
        if start <= bad_month <= end:
            reply.status = 500
        else:
            reply.status = 200
            days = []
            month = datetime.datetime.strptime(start, "%Y/%m")
            while month <= datetime.datetime.strptime(end, "%Y/%m"):
                days += [month.replace(day=d).strftime("%Y/%m/%d") for d in (1, 2, 3)]
                month = (month + datetime.timedelta(days=32)).replace(day=1)
            reply.read = _json_body(
                {
                    "timeCurve": [
                        {
                            "cups": "ESXXXXXXXXXXXXXXXXTEST",
                            "date": day,
                            "time": "01:00",
                            "consumptionKWh": 1.0,
                            "obtainMethod": "Real",
                            "surplusEnergyKWh": 0,
                        }
                        for day in days
                    ],
                    "maxPower": [
                        {
                            "cups": "ESXXXXXXXXXXXXXXXXTEST",
                            "date": day,
                            "time": "12:00",
                            "maxPower": 2.5,
                            "period": "VALLE",
                        }
                        for day in days
                    ],
                }
            )
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=reply)
        context.__aexit__ = AsyncMock(return_value=False)
        return context

    return fake_get


@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_failing_month_does_not_lose_the_range(mock_token, tmp_path):
    """A month Datadis cannot serve only loses itself; rows stay sorted."""
    calls: list[tuple[str, str]] = []
    connector = DatadisConnector(
        MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path)
    )
    start = datetime.datetime(2022, 1, 15)
    end = datetime.datetime(2022, 4, 30)

    with patch(
        "aiohttp.ClientSession.get",
        side_effect=_first_month_broken_get("2022/01", calls),
    ):
        energy = connector.get_consumption_data(
            "ESXXXXXXXXXXXXXXXXTEST", "2", start, end, "0", 5
        )
    # range query and its retry, then month by month from the newest
    assert calls == [
        ("2022/01", "2022/04"),
        ("2022/01", "2022/04"),
        ("2022/04", "2022/04"),
        ("2022/03", "2022/03"),
        ("2022/02", "2022/02"),
        ("2022/01", "2022/01"),
        ("2022/01", "2022/01"),
    ]
    expected = [
        datetime.datetime(2022, month, day) for month in (2, 3, 4) for day in (1, 2, 3)
    ]
    assert [x.datetime for x in energy] == expected

    calls.clear()
    with patch(
        "aiohttp.ClientSession.get",
        side_effect=_first_month_broken_get("2022/01", calls),
    ):
        power = connector.get_max_power("ESXXXXXXXXXXXXXXXXTEST", "2", start, end)
    assert [x.datetime for x in power] == [x.replace(hour=12) for x in expected]

    # a second run the same day is answered from the cache, still sorted
    calls.clear()
    with patch(
        "aiohttp.ClientSession.get",
        side_effect=_first_month_broken_get("2022/01", calls),
    ):
        energy = connector.get_consumption_data(
            "ESXXXXXXXXXXXXXXXXTEST", "2", start, end, "0", 5
        )
    assert calls == []
    assert [x.datetime for x in energy] == expected


@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_healthy_range_is_a_single_query(mock_token, tmp_path):
    """Without failures the whole range is still fetched in one request."""
    calls: list[tuple[str, str]] = []
    connector = DatadisConnector(
        MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path)
    )

    with patch(
        "aiohttp.ClientSession.get",
        side_effect=_first_month_broken_get("2021/12", calls),
    ):
        energy = connector.get_consumption_data(
            "ESXXXXXXXXXXXXXXXXTEST",
            "2",
            datetime.datetime(2022, 1, 1),
            datetime.datetime(2022, 4, 30),
            "0",
            5,
        )

    assert calls == [("2022/01", "2022/04")]
    assert len(energy) == 12
    assert [x.datetime for x in energy] == sorted(x.datetime for x in energy)


@patch.object(
    DatadisConnector, "_async_get_token", new_callable=AsyncMock, return_value=True
)
def test_failing_single_month_is_not_retried_by_month(mock_token, tmp_path):
    """A single-month query has nothing to split."""
    calls: list[tuple[str, str]] = []
    connector = DatadisConnector(
        MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path)
    )

    with patch(
        "aiohttp.ClientSession.get",
        side_effect=_first_month_broken_get("2022/01", calls),
    ):
        energy = connector.get_consumption_data(
            "ESXXXXXXXXXXXXXXXXTEST",
            "2",
            datetime.datetime(2022, 1, 15),
            datetime.datetime(2022, 1, 31),
            "0",
            5,
        )

    assert energy == []
    assert calls == [("2022/01", "2022/01"), ("2022/01", "2022/01")]


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (200, "token", LoginStatus.OK),
        # what Datadis answers to wrong credentials (checked 2026-10)
        (
            500,
            '{"status": 500, "error": "Internal Server Error", "message": "Bad credentials"}',
            LoginStatus.REJECTED,
        ),
        (401, "", LoginStatus.REJECTED),
        (403, "Forbidden", LoginStatus.REJECTED),
        (500, '{"status": 500, "message": "Server got itself in trouble"}', LoginStatus.UNAVAILABLE),
        (503, "Service Unavailable", LoginStatus.UNAVAILABLE),
    ],
)
def test_login_status(status, body, expected, tmp_path):
    """A login tells rejected credentials apart from Datadis failing."""
    reply = MagicMock()
    reply.status = status
    reply.text = AsyncMock(return_value=body)
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path))
    assert connector.login_status is None

    with patch("aiohttp.ClientSession.post") as post:
        post.return_value.__aenter__.return_value = reply
        assert connector.login() is (expected is LoginStatus.OK)

    assert connector.login_status is expected


def test_login_status_on_connection_error(tmp_path):
    """No answer at all is not taken as rejected credentials."""
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path))

    with patch("aiohttp.ClientSession.post", side_effect=aiohttp.ClientConnectionError):
        assert connector.login() is False

    assert connector.login_status is LoginStatus.UNAVAILABLE


@patch("aiohttp.ClientSession.get")
def test_login_status_after_a_rejected_refresh(mock_get, tmp_path):
    """A query whose token refresh is rejected leaves the status as rejected."""
    expired = MagicMock()
    expired.status = 401
    expired.text = AsyncMock(return_value="")
    mock_get.return_value.__aenter__.return_value = expired
    rejected = MagicMock()
    rejected.status = 500
    rejected.text = AsyncMock(return_value='{"message": "Bad credentials"}')
    connector = DatadisConnector(MOCK_USERNAME, MOCK_PASSWORD, storage_path=str(tmp_path))

    with patch("aiohttp.ClientSession.post") as post:
        post.return_value.__aenter__.return_value = rejected
        assert connector.get_supplies() == []

    assert connector.login_status is LoginStatus.REJECTED
