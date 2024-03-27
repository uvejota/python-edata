"""Datadis API connector.

To fetch data from datadis.es private API.
There a few issues that are workarounded:
 - You have to wait 24h between two identical requests.
 - Datadis server does not like ranges greater than 1 month.
 - TODO: gzip header checksum errors under weird circumstances
"""

import contextlib
import hashlib
import json
import logging
from datetime import datetime, timedelta
import os
from dateutil.relativedelta import relativedelta

import requests

from ..definitions import ConsumptionData, ContractData, MaxPowerData, SupplyData
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
RECENT_QUERIES_FILENAME = "edata_recent_queries.json"
RECENT_QUERIES_CACHE_FILENAME = "edata_recent_queries_cache.json"
DEFAULT_RECENT_QUERIES_FILE = f"/tmp/{RECENT_QUERIES_FILENAME}"
DEFAULT_RECENT_QUERIES_CACHE = f"/tmp/{RECENT_QUERIES_CACHE_FILENAME}"


class DatadisConnector:
    """A Datadis private API connector."""

    def __init__(
        self,
        username: str,
        password: str,
        enable_smart_fetch: bool = True,
        storage_path: str | None = None,
    ) -> None:
        """DatadisConnector constructor."""

        # initialize some things
        self._usr = username
        self._pwd = password
        self._session = requests.Session()
        self._token = {}
        self._smart_fetch = enable_smart_fetch
        self._recent_queries = {}
        self._recent_cache = {}
        if storage_path is not None:
            self._recent_queries_file = os.path.join(
                storage_path, RECENT_QUERIES_FILENAME
            )
            self._recent_queries_cache_file = os.path.join(
                storage_path, RECENT_QUERIES_CACHE_FILENAME
            )
        else:
            self._recent_queries_file = DEFAULT_RECENT_QUERIES_FILE
            self._recent_queries_cache_file = DEFAULT_RECENT_QUERIES_CACHE
        self._warned_queries = []

        # load caches (to avoid query spam if the program restarts)
        with contextlib.suppress(FileNotFoundError):
            with open(self._recent_queries_file, encoding="utf8") as dst_file:
                self._recent_queries = json.load(dst_file)
                for query in self._recent_queries:
                    self._recent_queries[query] = datetime.fromisoformat(
                        self._recent_queries[query]
                    )
            with open(self._recent_queries_cache_file, encoding="utf8") as dst_file:
                self._recent_cache = json.load(dst_file)

    def _update_recent_queries(self, query: str, data: dict | None = None) -> None:
        """Cache a successful query to avoid exceeding query limits."""

        # identify the query by a md5 hash
        hash_query = hashlib.md5(query.encode()).hexdigest()
        # prepare key (hash) and values (timestamp and response)
        self._recent_queries[hash_query] = datetime.now()
        if data is not None:
            self._recent_cache[hash_query] = data

        # purge old cache
        to_delete = []
        for _query in self._recent_queries:
            if (datetime.now() - self._recent_queries[_query]) > QUERY_LIMIT:
                to_delete.append(_query)

        for key in to_delete:
            with contextlib.suppress(KeyError):
                self._recent_queries.pop(key, None)
                self._recent_cache.pop(key, None)

        # dump current cache to disk
        try:
            with open(self._recent_queries_file, "w", encoding="utf8") as dst_file:
                json.dump(utils.serialize_dict(self._recent_queries), dst_file)
            if data is not None:
                with open(
                    self._recent_queries_cache_file, "w", encoding="utf8"
                ) as dst_file:
                    json.dump(self._recent_cache, dst_file)
        except Exception as e:
            _LOGGER.warning("Unknown error while updating cache: %s", e)

    def _is_recent_query(self, query: str) -> bool:
        """Check if a query has been done recently to avoid exceeding query limits."""
        hash_query = hashlib.md5(query.encode()).hexdigest()

        if hash_query in self._recent_queries:
            return (datetime.now() - self._recent_queries[hash_query]) < QUERY_LIMIT
        return False

    def _get_cache_for_query(self, query: str) -> dict:
        """Return cached response for a query."""
        hash_query = hashlib.md5(query.encode()).hexdigest()

        return self._recent_cache.get(hash_query, None)

    def _get_token(self):
        """Private method that fetches a new token if needed."""

        _LOGGER.info("No token found, fetching a new one")
        is_valid_token = False
        self._session = requests.Session()
        response = self._session.post(
            URL_TOKEN,
            data={
                TOKEN_USERNAME: self._usr.encode("utf-8"),
                TOKEN_PASSWD: self._pwd.encode("utf-8"),
            },
        )
        if response.status_code == 200:
            # store token encoded
            self._token["encoded"] = response.text
            # prepare session authorization bearer
            self._session.headers["Authorization"] = "Bearer " + self._token["encoded"]
            is_valid_token = True
        else:
            _LOGGER.error("Unknown error while retrieving token, got %s", response.text)
        return is_valid_token

    def login(self):
        """Test to login with provided credentials."""
        return self._get_token()

    def _get(
        self,
        url: str,
        request_data: dict | None = None,
        refresh_token: bool = False,
        is_retry: bool = False,
        ignore_recent_queries: bool = False,
    ):
        """Get request for Datadis API."""

        if request_data is None:
            data = {}
        else:
            data = request_data

        # refresh token if needed (recursive approach)
        is_valid_token = False
        response = []
        if refresh_token:
            is_valid_token = self._get_token()
        if is_valid_token or not refresh_token:
            # build get parameters
            params = "?" if len(data) > 0 else ""
            for param in data:
                key = param
                value = data[param]
                params = params + f"{key}={value}&"

            # check if query is already in cache
            if not ignore_recent_queries and self._is_recent_query(url + params):
                _cache = self._get_cache_for_query(url + params)
                if _cache is not None:
                    return _cache
                return []

            # run the query
            try:
                _LOGGER.debug("GET %s", url + params)
                reply = self._session.get(
                    url + params,
                    headers={"Accept-Encoding": "identity"},
                    timeout=TIMEOUT,
                )
            except requests.exceptions.Timeout:
                _LOGGER.warning("Timeout at %s", url + params)
                return []

            # eval response
            if reply.status_code == 200:
                # we're here if reply seems valid
                _LOGGER.info("Got 200 OK at %s", url + params)
                if reply.json():
                    response = reply.json()
                    self._update_recent_queries(url + params, response)
                else:
                    # this mostly happens when datadis provides an empty response
                    _LOGGER.info(
                        "Datadis returned an empty response at %s", url + params
                    )
                    self._update_recent_queries(url + params)
            elif reply.status_code == 401 and not refresh_token:
                # we're here if we were unauthorized so we will refresh the token
                response = self._get(
                    url,
                    request_data=data,
                    refresh_token=True,
                    ignore_recent_queries=ignore_recent_queries,
                )
            elif reply.status_code == 429:
                # we're here if we exceeded datadis API rates (24h)
                _LOGGER.warning(
                    "%s %s at %s",
                    reply.status_code,
                    reply.text,
                    url + params,
                )
                self._update_recent_queries(url + params)
            elif is_retry:
                # otherwise, if this was a retried request... warn the user
                if (url + params) not in self._warned_queries:
                    _LOGGER.warning(
                        "%s %s at %s. %s. %s",
                        reply.status_code,
                        reply.text,
                        url + params,
                        "Query temporary disabled",
                        "Future 500 code errors for this query will be silenced until restart",
                    )
                self._update_recent_queries(url + params)
                self._warned_queries.append(url + params)
            else:
                # finally, retry since an unexpected error took place (mostly 500 errors - server fault)
                response = self._get(
                    url,
                    request_data,
                    is_retry=True,
                    ignore_recent_queries=ignore_recent_queries,
                )

        return response

    def get_supplies(self, authorized_nif: str | None = None):
        """Datadis 'get_supplies' query."""

        data = {}

        # If authorized_nif is provided, we have to include it as parameter
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif

        # Request the resource
        response = self._get(
            URL_GET_SUPPLIES, request_data=data, ignore_recent_queries=False
        )

        # Response is a list of serialized supplies.
        # We will iter through them to transform them into SupplyData objects
        supplies = []
        # Build tomorrow Y/m/d string since we will use it as the 'date_end' of
        # active supplies
        tomorrow_str = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d")
        for i in response:
            # check data integrity (maybe this can be supressed if datadis proves to be reliable)
            if all(k in i for k in GET_SUPPLIES_MANDATORY_FIELDS):
                supplies.append(
                    SupplyData(
                        cups=i["cups"],  # the supply identifier
                        date_start=datetime.strptime(
                            i["validDateFrom"]
                            if i["validDateFrom"] != ""
                            else "1970/01/01",
                            "%Y/%m/%d",
                        ),  # start date of the supply. 1970/01/01 if unset.
                        date_end=datetime.strptime(
                            i["validDateTo"]
                            if i["validDateTo"] != ""
                            else tomorrow_str,
                            "%Y/%m/%d",
                        ),  # end date of the supply, tomorrow if unset
                        # the following parameters are not crucial, so they can be none
                        address=i["address"] if "address" in i else None,
                        postal_code=i["postalCode"] if "postalCode" in i else None,
                        province=i["province"] if "province" in i else None,
                        municipality=i["municipality"] if "municipality" in i else None,
                        distributor=i["distributor"] if "distributor" in i else None,
                        # these two are mandatory, we will use them to fetch contracts data
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

    def get_contract_detail(
        self, cups: str, distributor_code: str, authorized_nif: str | None = None
    ):
        """Datadis get_contract_detail query."""
        data = {"cups": cups, "distributorCode": distributor_code}
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        response = self._get(
            URL_GET_CONTRACT_DETAIL, request_data=data, ignore_recent_queries=False
        )
        contracts = []
        tomorrow_str = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d")
        for i in response:
            if all(k in i for k in GET_CONTRACT_DETAIL_MANDATORY_FIELDS):
                contracts.append(
                    ContractData(
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
                        power_p1=i["contractedPowerkW"][0]
                        if isinstance(i["contractedPowerkW"], list)
                        else None,
                        power_p2=i["contractedPowerkW"][1]
                        if (len(i["contractedPowerkW"]) > 1)
                        else None,
                    )
                )
            else:
                _LOGGER.warning(
                    "Weird data structure while fetching contracts data, got %s",
                    response,
                )
        return contracts

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
        """Datadis get_consumption_data query."""

        if self._smart_fetch and not is_smart_fetch:
            _start = start_date
            consumptions = []
            while _start < end_date:
                _end = min(
                    _start + relativedelta(months=MAX_CONSUMPTIONS_MONTHS), end_date
                )
                consumptions = utils.extend_by_key(
                    consumptions,
                    self.get_consumption_data(
                        cups,
                        distributor_code,
                        _start,
                        _end,
                        measurement_type,
                        point_type,
                        authorized_nif,
                        is_smart_fetch=True,
                    ),
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

        response = self._get(URL_GET_CONSUMPTION_DATA, request_data=data)

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
                        ConsumptionData(
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

    def get_max_power(
        self,
        cups: str,
        distributor_code: str,
        start_date: datetime,
        end_date: datetime,
        authorized_nif: str | None = None,
    ):
        """Datadis get_max_power query."""

        data = {
            "cups": cups,
            "distributorCode": distributor_code,
            "startDate": datetime.strftime(start_date, "%Y/%m"),
            "endDate": datetime.strftime(end_date, "%Y/%m"),
        }
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        response = self._get(URL_GET_MAX_POWER, request_data=data)
        maxpower_values = []
        for i in response:
            if all(k in i for k in GET_MAX_POWER_MANDATORY_FIELDS):
                maxpower_values.append(
                    MaxPowerData(
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
