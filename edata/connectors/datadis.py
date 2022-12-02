"""Definitions for API connectors"""

import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
from dateutil.relativedelta import relativedelta

from ..definitions import ConsumptionData, ContractData, MaxPowerData, SupplyData
from ..processors import utils

_LOGGER = logging.getLogger(__name__)

URL_TOKEN = "https://datadis.es/nikola-auth/tokens/login"
TOKEN_USERNAME = "username"
TOKEN_PASSWD = "password"
URL_GET_SUPPLIES = "https://datadis.es/api-private/api/get-supplies"
GET_SUPPLIES_MANDATORY_FIELDS = [
    "cups",
    "validDateFrom",
    "validDateTo",
    "pointType",
    "distributorCode",
]
URL_GET_CONTRACT_DETAIL = "https://datadis.es/api-private/api/get-contract-detail"
GET_CONTRACT_DETAIL_MANDATORY_FIELDS = [
    "startDate",
    "endDate",
    "marketer",
    "contractedPowerkW",
]
URL_GET_CONSUMPTION_DATA = "https://datadis.es/api-private/api/get-consumption-data"
GET_CONSUMPTION_DATA_MANDATORY_FIELDS = [
    "time",
    "date",
    "consumptionKWh",
    "obtainMethod",
]
URL_GET_MAX_POWER = "https://datadis.es/api-private/api/get-max-power"
GET_MAX_POWER_MANDATORY_FIELDS = ["time", "date", "maxPower"]

TIMEOUT = 30

MAX_CONSUMPTIONS_MONTHS = 2

QUERY_LIMIT = timedelta(hours=24)

RECENT_QUERIES_FILE = "/tmp/edata_recent_queries.json"


class DatadisConnector:
    """A Datadis private API connector"""

    def __init__(
        self,
        username: str,
        password: str,
        enable_smart_fetch: bool = True,
    ) -> None:
        """Init method"""

        self._usr = username
        self._pwd = password
        self._session = requests.Session()
        self._token = {}
        self._smart_fetch = enable_smart_fetch

        try:
            with open(RECENT_QUERIES_FILE, "r") as dst_file:
                self._recent_queries = json.load(dst_file)
                for query in self._recent_queries:
                    self._recent_queries[query] = datetime.fromisoformat(
                        self._recent_queries[query]
                    )
        except Exception:
            self._recent_queries = {}

    def _update_recent_queries(self, query: str) -> None:
        """Records a recent successful query to avoid exceeding query limits"""

        hash_query = hashlib.md5(query.encode()).hexdigest()
        self._recent_queries[hash_query] = datetime.now()

        # purge old queries
        to_delete = []
        for _query in self._recent_queries:
            if (datetime.now() - self._recent_queries[_query]) > QUERY_LIMIT:
                to_delete.append(_query)

        for key in to_delete:
            self._recent_queries.pop(key, None)

        try:
            with open(RECENT_QUERIES_FILE, "w") as dst_file:
                json.dump(utils.serialize_dict(self._recent_queries), dst_file)
        except Exception:
            pass

    def _is_recent_query(self, query: str) -> bool:
        """Checks if a query has been done recently to avoid exceeding query limits"""
        hash_query = hashlib.md5(query.encode()).hexdigest()

        if hash_query in self._recent_queries:
            return (datetime.now() - self._recent_queries[hash_query]) < QUERY_LIMIT
        return False

    def _get_token(self):
        """Private method that fetches a new token if needed"""

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
            _LOGGER.debug("token received")
            is_valid_token = True
        else:
            _LOGGER.error("Unknown error while retrieving token, got %s", response.text)
        return is_valid_token

    def login(self):
        """Test to login with provided credentials"""
        return self._get_token()

    def _send_cmd(
        self,
        url,
        request_data=None,
        refresh_token=False,
        is_retry=False,
        ignore_recent_queries=False,
    ):
        """Common method for GET requests"""

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
            # query

            if not ignore_recent_queries and self._is_recent_query(url + params):
                return response

            try:
                _LOGGER.info("GET %s", url + params)
                reply = self._session.get(url + params, timeout=TIMEOUT)
            except requests.exceptions.Timeout:
                _LOGGER.warning("Timeout at %s", url + params)
                return response

            # eval response
            if reply.status_code == 200 and reply.json():
                _LOGGER.info("Got 200 OK at %s", url + params)
                response = reply.json()

                self._update_recent_queries(url + params)
            elif reply.status_code == 401 and not refresh_token:
                response = self._send_cmd(
                    url,
                    request_data=data,
                    refresh_token=True,
                    ignore_recent_queries=ignore_recent_queries,
                )
            elif reply.status_code == 429:
                _LOGGER.warning(
                    "%s %s at %s",
                    reply.status_code,
                    reply.text,
                    url + params,
                )
                self._update_recent_queries(url + params)
            elif reply.status_code == 200:
                _LOGGER.info(
                    "%s returned an empty response, try again later", url + params
                )
                self._update_recent_queries(url + params)
            else:
                if is_retry:
                    _LOGGER.error(
                        "%s %s at %s",
                        reply.status_code,
                        reply.text,
                        url + params,
                    )
                else:
                    self._send_cmd(url, request_data, is_retry=True)
        return response

    def get_supplies(self, authorized_nif=None):
        """Datadis get_supplies query"""
        data = {}
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        response = self._send_cmd(
            URL_GET_SUPPLIES, request_data=data, ignore_recent_queries=True
        )
        supplies = []
        tomorrow_str = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d")
        for i in response:
            if all(k in i for k in GET_SUPPLIES_MANDATORY_FIELDS):
                supplies.append(
                    SupplyData(
                        cups=i["cups"],
                        date_start=datetime.strptime(
                            i["validDateFrom"]
                            if i["validDateFrom"] != ""
                            else "1970/01/01",
                            "%Y/%m/%d",
                        ),
                        date_end=datetime.strptime(
                            i["validDateTo"]
                            if i["validDateTo"] != ""
                            else tomorrow_str,
                            "%Y/%m/%d",
                        ),
                        address=i["address"] if "address" in i else None,
                        postal_code=i["postalCode"] if "postalCode" in i else None,
                        province=i["province"] if "province" in i else None,
                        municipality=i["municipality"] if "municipality" in i else None,
                        distributor=i["distributor"] if "distributor" in i else None,
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

    def get_contract_detail(self, cups, distributor_code, authorized_nif=None):
        """Datadis get_contract_detail query"""
        data = {"cups": cups, "distributorCode": distributor_code}
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        response = self._send_cmd(
            URL_GET_CONTRACT_DETAIL, request_data=data, ignore_recent_queries=True
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
        cups,
        distributor_code,
        start_date,
        end_date,
        measurement_type,
        point_type,
        authorized_nif=None,
        is_smart_fetch=False,
    ):
        """Datadis get_consumption_data query"""

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

        response = self._send_cmd(URL_GET_CONSUMPTION_DATA, request_data=data)

        consumptions = []
        for i in response:
            if i.get("consumptionKWh", 0) > 0:
                if all(k in i for k in GET_CONSUMPTION_DATA_MANDATORY_FIELDS):
                    hour = str(int(i["time"].split(":")[0]) - 1)
                    date_as_dt = datetime.strptime(
                        f"{i['date']} {hour.zfill(2)}:00", "%Y/%m/%d %H:%M"
                    )
                    if not (start_date <= date_as_dt <= end_date):
                        continue  # skip element if dt is out of range
                    consumptions.append(
                        ConsumptionData(
                            datetime=date_as_dt,
                            delta_h=1,
                            value_kWh=i["consumptionKWh"],
                            real=True if i["obtainMethod"] == "Real" else False,
                        )
                    )
                else:
                    _LOGGER.warning(
                        "Weird data structure while fetching consumption data, got %s",
                        response,
                    )
        return consumptions

    def get_max_power(
        self, cups, distributor_code, start_date, end_date, authorized_nif=None
    ):
        """Datadis get_max_power query"""

        data = {
            "cups": cups,
            "distributorCode": distributor_code,
            "startDate": datetime.strftime(start_date, "%Y/%m"),
            "endDate": datetime.strftime(end_date, "%Y/%m"),
        }
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        response = self._send_cmd(URL_GET_MAX_POWER, request_data=data)
        maxpower_values = []
        for i in response:
            if all(k in i for k in GET_MAX_POWER_MANDATORY_FIELDS):
                maxpower_values.append(
                    MaxPowerData(
                        datetime=datetime.strptime(
                            f"{i['date']} {i['time']}", "%Y/%m/%d %H:%M"
                        )
                        if "date" in i and "time" in i
                        else None,
                        value_kW=i["maxPower"] if "maxPower" in i else None,
                    )
                )
            else:
                _LOGGER.warning(
                    "Weird data structure while fetching maximeter data, got %s",
                    response,
                )
        return maxpower_values
