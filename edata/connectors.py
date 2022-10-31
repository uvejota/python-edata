"""Definitions for API connectors"""

import logging
from datetime import datetime, timedelta

import requests

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


class DatadisConnector:
    """A Datadis private API connector"""

    SCOPE = ["supplies", "contracts", "consumptions", "maximeter"]
    UPDATE_INTERVAL = timedelta(hours=24)
    SECURE_FETCH_THRESHOLD = 1

    def __init__(
        self,
        username: str,
        password: str,
        log_level=logging.WARNING,
    ):
        logging.getLogger().setLevel(log_level)
        self._usr = username
        self._pwd = password
        self._session = requests.Session()
        self._token = {}

    def _get_token(self):
        _LOGGER.info("No token found, fetching a new one")
        is_valid_token = False
        self._session = requests.Session()
        credentials = {
            "username": self._usr.encode("utf-8"),
            "password": self._pwd.encode("utf-8"),
        }
        r = self._session.post(
            "https://datadis.es/nikola-auth/tokens/login", data=credentials
        )
        if r.status_code == 200:
            # store token encoded
            self._token["encoded"] = r.text
            # prepare session authorization bearer
            self._session.headers["Authorization"] = "Bearer " + self._token["encoded"]
            _LOGGER.debug("token received")
            is_valid_token = True
        else:
            _LOGGER.error("Unknown error while retrieving token, got %s", r.text)
        return is_valid_token

    def _send_cmd(self, url, data={}, refresh_token=False):
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
            r = self._session.get(url + params, timeout=15)
            # eval response
            if r.status_code == 200 and r.json():
                _LOGGER.debug("got a valid response for %s", url + params)
                response = r.json()
            elif r.status_code == 401 and not refresh_token:
                response = self._send_cmd(url, data=data, refresh_token=True)
            elif r.status_code == 200:
                _LOGGER.info(
                    "%s returned an empty response, try again later", url + params
                )
            else:
                _LOGGER.error(
                    "%s returned %s with code %s",
                    url + params,
                    r.text,
                    r.status_code,
                )
        return response

    def get_supplies(self, authorized_nif=None):
        data = {}
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        r = self._send_cmd("https://datadis.es/api-private/api/get-supplies", data=data)
        c = []
        tomorrow_str = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d")
        for i in r:
            if all(
                k in i
                for k in (
                    "cups",
                    "validDateFrom",
                    "validDateTo",
                    "pointType",
                    "distributorCode",
                )
            ):
                d = {
                    "cups": i["cups"],
                    "date_start": datetime.strptime(
                        i["validDateFrom"]
                        if i["validDateFrom"] != ""
                        else "1970/01/01",
                        "%Y/%m/%d",
                    ),
                    "date_end": datetime.strptime(
                        i["validDateTo"] if i["validDateTo"] != "" else tomorrow_str,
                        "%Y/%m/%d",
                    ),
                    "address": i["address"] if "address" in i else None,
                    "postal_code": i["postalCode"] if "postalCode" in i else None,
                    "province": i["province"] if "province" in i else None,
                    "municipality": i["municipality"] if "municipality" in i else None,
                    "distributor": i["distributor"] if "distributor" in i else None,
                    "pointType": i["pointType"],
                    "distributorCode": i["distributorCode"],
                }
                c.append(d)
            else:
                _LOGGER.warning(
                    "Weird data structure while fetching supplies data, got %s", r
                )
        return c

    def get_contract_detail(self, cups, distributor_code, authorized_nif=None):
        data = {"cups": cups, "distributorCode": distributor_code}
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        r = self._send_cmd(
            "https://datadis.es/api-private/api/get-contract-detail", data=data
        )
        c = []
        tomorrow_str = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d")
        for i in r:
            if all(
                k in i
                for k in ("startDate", "endDate", "marketer", "contractedPowerkW")
            ):
                d = {
                    "date_start": datetime.strptime(
                        i["startDate"] if i["startDate"] != "" else "1970/01/01",
                        "%Y/%m/%d",
                    ),
                    "date_end": datetime.strptime(
                        i["endDate"] if i["endDate"] != "" else tomorrow_str, "%Y/%m/%d"
                    ),
                    "marketer": i["marketer"],
                    "distributorCode": distributor_code,
                    "power_p1": i["contractedPowerkW"][0]
                    if isinstance(i["contractedPowerkW"], list)
                    else None,
                    "power_p2": i["contractedPowerkW"][1]
                    if (len(i["contractedPowerkW"]) > 1)
                    else None,
                }
                c.append(d)
            else:
                _LOGGER.warning(
                    "Weird data structure while fetching contracts data, got %s", r
                )
        return c

    def get_consumption_data(
        self,
        cups,
        distributor_code,
        start_date,
        end_date,
        measurement_type,
        point_type,
        authorized_nif=None,
    ):
        _LOGGER.info("Fetching consumptions from %s to %s", start_date, end_date)
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
        r = self._send_cmd(
            "https://datadis.es/api-private/api/get-consumption-data", data=data
        )
        c = []
        for i in r:
            if i.get("consumptionKWh", 0) > 0:
                if all(
                    k in i for k in ("time", "date", "consumptionKWh", "obtainMethod")
                ):
                    hour = str(int(i["time"].split(":")[0]) - 1)
                    date_as_dt = datetime.strptime(
                        f"{i['date']} {hour.zfill(2)}:00", "%Y/%m/%d %H:%M"
                    )
                    if not (start_date <= date_as_dt and date_as_dt <= end_date):
                        continue  # skip element if dt is out of range
                    d = {
                        "datetime": date_as_dt,
                        "delta_h": 1,
                        "value_kWh": i["consumptionKWh"],
                        "real": True if i["obtainMethod"] == "Real" else False,
                    }
                    c.append(d)
                else:
                    _LOGGER.warning(
                        "Weird data structure while fetching consumption data, got %s",
                        r,
                    )
        return c

    def get_max_power(
        self, cups, distributor_code, start_date, end_date, authorized_nif=None
    ):
        _LOGGER.info("Fetching maximeter from %s to %s", start_date, end_date)
        data = {
            "cups": cups,
            "distributorCode": distributor_code,
            "startDate": datetime.strftime(start_date, "%Y/%m"),
            "endDate": datetime.strftime(end_date, "%Y/%m"),
        }
        if authorized_nif is not None:
            data["authorizedNif"] = authorized_nif
        r = self._send_cmd(
            "https://datadis.es/api-private/api/get-max-power", data=data
        )
        c = []
        for i in r:
            if all(k in i for k in ("time", "date", "maxPower")):
                d = {
                    "datetime": datetime.strptime(
                        f"{i['date']} {i['time']}", "%Y/%m/%d %H:%M"
                    )
                    if "date" in i and "time" in i
                    else None,
                    "value_kW": i["maxPower"] if "maxPower" in i else None,
                }
                c.append(d)
            else:
                _LOGGER.warning(
                    "Weird data structure while fetching maximeter data, got %s", r
                )
        return c
