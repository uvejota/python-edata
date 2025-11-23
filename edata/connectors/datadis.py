"""Datadis API connector.

To fetch data from datadis.es private API.
There a few issues that are workarounded:
 - You have to wait 24h between two identical requests.
 - Datadis server does not like ranges greater than 1 month.
"""

import contextlib
from datetime import datetime, timedelta

import hashlib
import logging
import os
import tempfile
import typing
import diskcache

from dateutil.relativedelta import relativedelta

import aiohttp
import asyncio

from edata.models import Supply, Contract, Energy, Power
from ..processors import utils

_LOGGER = logging.getLogger(__name__)

# Token-related constants
URL_TOKEN = "https://datadis.es/nikola-auth/tokens/login"
TOKEN_USERNAME = "username"
TOKEN_PASSWD = "password"

# Supplies-related constants
URL_GET_SUPPLIES = "https://datadis.es/api-private/api/get-supplies"
GET_SUPPLIES_MANDATORY_FIELDS = [
    "cups",
    "validDateFrom",
    "validDateTo",
    "pointType",
    "distributorCode",
]

# Contracts-related constants
URL_GET_CONTRACT_DETAIL = "https://datadis.es/api-private/api/get-contract-detail"
GET_CONTRACT_DETAIL_MANDATORY_FIELDS = [
    "startDate",
    "endDate",
    "marketer",
    "contractedPowerkW",
]

# Consumption-related constants
URL_GET_CONSUMPTION_DATA = "https://datadis.es/api-private/api/get-consumption-data"
GET_CONSUMPTION_DATA_MANDATORY_FIELDS = [
    "time",
    "date",
    "consumptionKWh",
    "obtainMethod",
]
MAX_CONSUMPTIONS_MONTHS = (
    1  # max consumptions in a single request (fixed to 1 due to datadis limitations)
)

# Maximeter-related constants
URL_GET_MAX_POWER = "https://datadis.es/api-private/api/get-max-power"
GET_MAX_POWER_MANDATORY_FIELDS = ["time", "date", "maxPower"]

# Timing constants
TIMEOUT = 3 * 60  # requests timeout
QUERY_LIMIT = timedelta(hours=24)  # a datadis limitation, again...


# Cache-related constants
RECENT_CACHE_SUBDIR = "cache"



def migrate_storage(storage_dir):
    """Migrate storage from older versions."""
    with contextlib.suppress(FileNotFoundError):
        os.remove(os.path.join(storage_dir, "edata_recent_queries.json"))
        os.remove(os.path.join(storage_dir, "edata_recent_queries_cache.json"))



class DatadisConnector:
    """A Datadis private API connector."""

    def __init__(
        self,
        username: str,
        password: str,
        enable_smart_fetch: bool = True,
        storage_path: str | None = None,
    ) -> None:
        self._usr = username
        self._pwd = password
        self._token = {}
        self._smart_fetch = enable_smart_fetch
        self._recent_queries = {}
        self._recent_cache = {}
        self._warned_queries = []
        if storage_path is not None:
            self._recent_cache_dir = os.path.join(storage_path, RECENT_CACHE_SUBDIR)
            migrate_storage(storage_path)
        else:
            self._recent_cache_dir = os.path.join(
                tempfile.gettempdir(), RECENT_CACHE_SUBDIR
            )
        os.makedirs(self._recent_cache_dir, exist_ok=True)
        self._cache = diskcache.Cache(self._recent_cache_dir)

    def _update_recent_queries(self, query: str, data: dict | None = None) -> None:
        """Cache a successful query to avoid exceeding query limits (diskcache)."""
        hash_query = hashlib.md5(query.encode()).hexdigest()
        try:
            self._cache.set(hash_query, data, expire=QUERY_LIMIT.total_seconds())
            _LOGGER.info("Updating cache item '%s'", hash_query)
        except Exception as e:
            _LOGGER.warning("Unknown error while updating cache: %s", e)

    def _is_recent_query(self, query: str) -> bool:
        """Check if a query has been done recently to avoid exceeding query limits (diskcache)."""
        hash_query = hashlib.md5(query.encode()).hexdigest()
        return hash_query in self._cache

    def _get_cache_for_query(self, query: str):
        """Return cached response for a query (diskcache)."""
        hash_query = hashlib.md5(query.encode()).hexdigest()
        try:
            return self._cache.get(hash_query, default=None)
        except Exception:
            return None


    async def _async_get_token(self):
        """Private async method that fetches a new token if needed."""
        _LOGGER.info("No token found, fetching a new one")
        is_valid_token = False
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(
                    URL_TOKEN,
                    data={
                        TOKEN_USERNAME: self._usr,
                        TOKEN_PASSWD: self._pwd,
                    },
                ) as response:
                    text = await response.text()
                    if response.status == 200:
                        self._token["encoded"] = text
                        self._token["headers"] = {"Authorization": "Bearer " + self._token["encoded"]}
                        is_valid_token = True
                    else:
                        _LOGGER.error("Unknown error while retrieving token, got %s", text)
            except Exception as e:
                _LOGGER.error("Exception while retrieving token: %s", e)
        return is_valid_token


    def login(self):
        """Test to login with provided credentials (sync wrapper)."""
        return asyncio.run(self._async_get_token())


    async def _async_get(
        self,
        url: str,
        request_data: dict | None = None,
        refresh_token: bool = False,
        is_retry: bool = False,
        ignore_recent_queries: bool = False,
    ) -> list[dict[str,typing.Any]]:
        """Async get request for Datadis API."""
        
        if request_data is None:
            data = {}
        else:
            data = request_data

        is_valid_token = False
        response = []
        if refresh_token:
            is_valid_token = await self._async_get_token()
        if is_valid_token or not refresh_token:
            params = "?" if len(data) > 0 else ""
            for param in data:
                key = param
                value = data[param]
                params = params + f"{key}={value}&"
            anonym_params = "?" if len(data) > 0 else ""
            for anonym_param in data:
                key = anonym_param
                if key == "cups":
                    value = "xxxx" + str(data[anonym_param])[-5:]
                elif key == "authorizedNif":
                    value = "xxxx"
                else:
                    value = data[anonym_param]
                anonym_params = anonym_params + f"{key}={value}&"

            if not ignore_recent_queries and self._is_recent_query(url + params):
                _cache = self._get_cache_for_query(url + params)
                if _cache is not None:
                    _LOGGER.info(
                        "Returning cached response for '%s'", url + anonym_params
                    )
                    return _cache # type: ignore
                return []

            try:
                _LOGGER.info("GET %s", url + anonym_params)
                headers = {"Accept-Encoding": "identity"}
                if self._token.get("headers"):
                    headers.update(self._token["headers"])
                timeout = aiohttp.ClientTimeout(total=TIMEOUT)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(
                        url + params,
                        headers=headers,
                    ) as reply:
                        text = await reply.text()
                        if reply.status == 200:
                            _LOGGER.info("Got 200 OK")
                            try:
                                json_data = await reply.json(content_type=None)
                                if json_data:
                                    response = json_data
                                    if not ignore_recent_queries:
                                        self._update_recent_queries(url + params, response)
                                else:
                                    _LOGGER.info("Got an empty response")
                                    if not ignore_recent_queries:
                                        self._update_recent_queries(url + params)
                            except Exception as e:
                                _LOGGER.warning("Failed to parse JSON response")
                        elif reply.status == 401 and not refresh_token:
                            response = await self._async_get(
                                url,
                                request_data=data,
                                refresh_token=True,
                                ignore_recent_queries=ignore_recent_queries,
                            )
                        elif reply.status == 429:
                            _LOGGER.warning(
                                "Got status code '%s' with message '%s'",
                                reply.status,
                                text,
                            )
                            if not ignore_recent_queries:
                                self._update_recent_queries(url + params)
                        elif is_retry:
                            if (url + params) not in self._warned_queries:
                                _LOGGER.warning(
                                    "Got status code '%s' with message '%s'. %s. %s",
                                    reply.status,
                                    text,
                                    "Query temporary disabled",
                                    "Future 500 code errors for this query will be silenced until restart",
                                )
                            if not ignore_recent_queries:
                                self._update_recent_queries(url + params)
                            self._warned_queries.append(url + params)
                        else:
                            response = await self._async_get(
                                url,
                                request_data,
                                is_retry=True,
                                ignore_recent_queries=ignore_recent_queries,
                            )
            except asyncio.TimeoutError:
                _LOGGER.warning("Timeout at %s", url + anonym_params)
                return []
            except Exception as e:
                _LOGGER.warning("Exception at %s: %s", url + anonym_params, e)
                return []
        return response

    async def async_get_supplies(self, authorized_nif: str | None = None) -> list[Supply]:
        data = {}
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        response = await self._async_get(
            URL_GET_SUPPLIES, request_data=data, ignore_recent_queries=True
        )
        supplies = []
        tomorrow_str = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d")
        for i in response:
            if all(k in i for k in GET_SUPPLIES_MANDATORY_FIELDS):
                supplies.append(
                    Supply(
                        cups=i["cups"],
                        date_start=datetime.strptime(
                            (
                                i["validDateFrom"]
                                if i["validDateFrom"] != ""
                                else "1970/01/01"
                            ),
                            "%Y/%m/%d",
                        ),
                        date_end=datetime.strptime(
                            (
                                i["validDateTo"]
                                if i["validDateTo"] != ""
                                else tomorrow_str
                            ),
                            "%Y/%m/%d",
                        ),
                        address=i.get("address", None),
                        postal_code=i.get("postalCode", None),
                        province=i.get("province", None),
                        municipality=i.get("municipality", None),
                        distributor=i.get("distributor", None),
                        pointType=i["pointType"],
                        distributorCode=i["distributorCode"],
                    )
                )
            else:
                _LOGGER.warning(
                    "Weird data structure while fetching supplies data, got %s",
                    response,
                )
        return supplies

    def get_supplies(self, authorized_nif: str | None = None):
        """Datadis 'get_supplies' query (sync wrapper)."""
        return asyncio.run(self.async_get_supplies(authorized_nif=authorized_nif))


    async def async_get_contract_detail(
        self, cups: str, distributor_code: str, authorized_nif: str | None = None
    ) -> list[Contract]:
        data = {"cups": cups, "distributorCode": distributor_code}
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        response = await self._async_get(
            URL_GET_CONTRACT_DETAIL, request_data=data, ignore_recent_queries=True
        )
        contracts = []
        tomorrow_str = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d")
        for i in response:
            if all(k in i for k in GET_CONTRACT_DETAIL_MANDATORY_FIELDS):
                contracts.append(
                    Contract(
                        date_start=datetime.strptime(
                            i["startDate"] if i["startDate"] != "" else "1970/01/01",
                            "%Y/%m/%d",
                        ),
                        date_end=datetime.strptime(
                            i["endDate"] if i["endDate"] != "" else tomorrow_str,
                            "%Y/%m/%d",
                        ),
                        marketer=i["marketer"],
                        distributorCode=distributor_code,
                        power_p1=(
                            i["contractedPowerkW"][0]
                            if isinstance(i["contractedPowerkW"], list)
                            else None
                        ),
                        power_p2=(
                            i["contractedPowerkW"][1]
                            if (len(i["contractedPowerkW"]) > 1)
                            else None
                        ),
                    )
                )
            else:
                _LOGGER.warning(
                    "Weird data structure while fetching contracts data, got %s",
                    response,
                )
        return contracts

    def get_contract_detail(
        self, cups: str, distributor_code: str, authorized_nif: str | None = None
    ):
        """Datadis get_contract_detail query (sync wrapper)."""
        return asyncio.run(self.async_get_contract_detail(cups, distributor_code, authorized_nif))


    async def async_get_consumption_data(
        self,
        cups: str,
        distributor_code: str,
        start_date: datetime,
        end_date: datetime,
        measurement_type: str,
        point_type: int,
        authorized_nif: str | None = None,
        is_smart_fetch: bool = False,
    ) -> list[Energy]:
        if self._smart_fetch and not is_smart_fetch:
            _start = start_date
            consumptions = []
            while _start < end_date:
                _end = min(
                    _start + relativedelta(months=MAX_CONSUMPTIONS_MONTHS), end_date
                )
                sub_consumptions = await self.async_get_consumption_data(
                    cups,
                    distributor_code,
                    _start,
                    _end,
                    measurement_type,
                    point_type,
                    authorized_nif,
                    is_smart_fetch=True,
                )
                consumptions = utils.extend_by_key(
                    consumptions,
                    sub_consumptions,
                    "datetime",
                )
                _start = _end
            return consumptions

        data = {
            "cups": cups,
            "distributorCode": distributor_code,
            "startDate": datetime.strftime(start_date, "%Y/%m"),
            "endDate": datetime.strftime(end_date, "%Y/%m"),
            "measurementType": measurement_type,
            "pointType": point_type,
        }
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif

        response = await self._async_get(URL_GET_CONSUMPTION_DATA, request_data=data)

        consumptions = []
        for i in response:
            if "consumptionKWh" in i:
                if all(k in i for k in GET_CONSUMPTION_DATA_MANDATORY_FIELDS):
                    hour = str(int(i["time"].split(":")[0]) - 1)
                    date_as_dt = datetime.strptime(
                        f"{i['date']} {hour.zfill(2)}:00", "%Y/%m/%d %H:%M"
                    )
                    if not (start_date <= date_as_dt <= end_date):
                        continue  # skip element if dt is out of range
                    _surplus = i.get("surplusEnergyKWh", 0)
                    if _surplus is None:
                        _surplus = 0
                    consumptions.append(
                        Energy(
                            datetime=date_as_dt,
                            delta_h=1,
                            value_kWh=i["consumptionKWh"],
                            surplus_kWh=_surplus,
                            real=i["obtainMethod"] == "Real",
                        )
                    )
                else:
                    _LOGGER.warning(
                        "Weird data structure while fetching consumption data, got %s",
                        response,
                    )
        return consumptions

    def get_consumption_data(
        self,
        cups: str,
        distributor_code: str,
        start_date: datetime,
        end_date: datetime,
        measurement_type: str,
        point_type: int,
        authorized_nif: str | None = None,
        is_smart_fetch: bool = False,
    ):
        """Datadis get_consumption_data query (sync wrapper)."""
        return asyncio.run(self.async_get_consumption_data(
            cups,
            distributor_code,
            start_date,
            end_date,
            measurement_type,
            point_type,
            authorized_nif,
            is_smart_fetch,
        ))


    async def async_get_max_power(
        self,
        cups: str,
        distributor_code: str,
        start_date: datetime,
        end_date: datetime,
        authorized_nif: str | None = None,
    )-> list[Power]:
        data = {
            "cups": cups,
            "distributorCode": distributor_code,
            "startDate": datetime.strftime(start_date, "%Y/%m"),
            "endDate": datetime.strftime(end_date, "%Y/%m"),
        }
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        response = await self._async_get(URL_GET_MAX_POWER, request_data=data)
        maxpower_values = []
        for i in response:
            if all(k in i for k in GET_MAX_POWER_MANDATORY_FIELDS):
                maxpower_values.append(
                    Power(
                        datetime=datetime.strptime(
                            f"{i['date']} {i['time']}", "%Y/%m/%d %H:%M"
                        ),
                        value_kW=i["maxPower"],
                    )
                )
            else:
                _LOGGER.warning(
                    "Weird data structure while fetching maximeter data, got %s",
                    response,
                )
        return maxpower_values

    def get_max_power(
        self,
        cups: str,
        distributor_code: str,
        start_date: datetime,
        end_date: datetime,
        authorized_nif: str | None = None,
    ):
        """Datadis get_max_power query (sync wrapper)."""
        return asyncio.run(self.async_get_max_power(
            cups,
            distributor_code,
            start_date,
            end_date,
            authorized_nif,
        ))
