from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from random import randint

import jwt
import pytz as tz
import requests
from aiopvpc import TARIFFS, PVPCData
from dateutil.relativedelta import relativedelta

from edata.const import (
    ERROR_CUPS,
    ERROR_LOGIN_AUTH,
    ERROR_SERVER_EMPTY,
    ERROR_SERVER_FAILURE,
    ERROR_TIMEOUT,
    anonymize,
)
from edata.data import (
    Consumption,
    Contract,
    Cost,
    DatadisData,
    EdataError,
    EsiosData,
    MaxPower,
    Supply,
    add_or_update,
    find_gaps,
)

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


class Connector:
    """Base class for connectors"""

    def __init__(self):
        self._data = {}
        self._last_updated = {}

    @property
    def data(self):
        return self._data

    @property
    def last_updated(self):
        return self._last_updated


class AuthError(EdataError):
    """Authentication error"""


class ServerError(EdataError):
    """Server error"""


class CUPSError(EdataError):
    """CUPS error"""


class TimeoutError(EdataError):
    """Timeout error"""


class DatadisConnector(Connector):
    UPDATE_INTERVAL = timedelta(minutes=60)
    SECURE_FETCH_THRESHOLD = 2
    _LABEL = "DatadisConnector"
    SENSIBLE_DATA = ["USERNAME", "PASSWORD", "CUPS"]

    def __init__(self, username, password):
        super().__init__()
        self._usr = username
        self._pwd = password
        self._session = requests.Session()
        self._retries = 0
        self._token = {}
        self._data = DatadisData(
            supplies=[], contracts=[], consumptions=[], maximeter=[]
        )
        self._last_try = datetime(1970, 1, 1)
        self._last_updated = {x: datetime(1970, 1, 1) for x in self._data}

    def login(self):
        is_valid_token = False
        self._session = requests.Session()
        credentials = {
            "username": self._usr.encode("utf-8"),
            "password": self._pwd.encode("utf-8"),
        }
        query = "https://datadis.es/nikola-auth/tokens/login"
        _LOGGER.info(f"{self._LABEL}: running login command...")
        r = self._session.post(query, data=credentials)
        if r.status_code == 200:
            # store token both encoded and decoded
            self._token["encoded"] = r.text
            self._token["decoded"] = jwt.decode(
                self._token["encoded"], options={"verify_signature": False}
            )
            # prepare session authorization bearer
            self._session.headers["Authorization"] = "Bearer " + self._token["encoded"]
            is_valid_token = True
        elif r.status_code == 500 and "credentials" in str(r.text):
            raise AuthError(self._LABEL, ERROR_LOGIN_AUTH(user=self._usr))
        else:
            raise ServerError(
                self._LABEL,
                ERROR_SERVER_FAILURE(user=self._usr, query=query, response=r.text),
            )
        return is_valid_token

    def _send_cmd(self, url, data={}, refresh_token=False, is_retry=False):
        # refresh token if needed (recursive approach)
        try:
            is_valid_token = False
            response = []
            if refresh_token:
                is_valid_token = self.login()
            if is_valid_token or not refresh_token:
                # build get parameters
                params = "?" if len(data) > 0 else ""
                for param in data:
                    key = param
                    value = data[param]
                    params = params + f"{key}={value}&"
                # query
                query = url + params
                _LOGGER.info(
                    f"{self._LABEL}: {'running' if not is_retry else 'retrying'} command {url.split('/')[-1]}{params}..."
                )
                try:
                    r = self._session.get(query, timeout=30)
                except requests.exceptions.ReadTimeout as e:
                    raise TimeoutError(
                        self._LABEL,
                        ERROR_TIMEOUT(seconds=30, user=self._usr, query=query),
                    )
                # eval response
                if r.status_code == 200 and r.json():
                    response = r.json()
                elif r.status_code == 401 and not refresh_token:
                    _LOGGER.info(f"{self._LABEL}: a new token is needed")
                    response = self._send_cmd(url, data=data, refresh_token=True)
                elif r.status_code == 200:
                    raise ServerError(
                        self._LABEL,
                        ERROR_SERVER_EMPTY(
                            user=self._usr, query=query, response=r.text
                        ),
                    )
                else:
                    raise ServerError(
                        self._LABEL,
                        ERROR_SERVER_FAILURE(
                            user=self._usr, query=query, response=r.text
                        ),
                    )
        except EdataError as e:
            if is_retry:
                raise e
            else:
                time.sleep(randint(2, 10))
                response = self._send_cmd(url, data=data, is_retry=True)
        return response

    def update(self, cups, date_from=None, date_to=None, ignore_interval=False):
        _LOGGER.info(
            f"{self._LABEL}: update requested for CUPS {anonymize(cups)} from {date_from} to {date_to}"
        )

        # check if update interval is not exhausted
        if (
            not ignore_interval
            and (datetime.now() - self._last_try) < self.UPDATE_INTERVAL
        ):
            _LOGGER.info(f"{self._LABEL}: skipping due to update interval")
            return False
        else:
            self._last_try = datetime.now()

        # default values are to fetch full history
        date_from = datetime(1970, 1, 1) if date_from is None else date_from
        date_to = datetime.today() if date_to is None else date_to

        # update supplies list
        self._update_supplies()
        for s in self._data["supplies"]:
            if s["cups"] == cups:
                _supply = s
                break
        else:
            raise CUPSError(
                self._LABEL,
                ERROR_CUPS(
                    cups=cups,
                    cups_list=", ".join(
                        [anonymize(x["cups"]) for x in self._data["supplies"]]
                    ),
                ),
            )

        # update contracts to get valid periods
        self._update_contracts(cups, _supply["distributor_code"])

        def fill_gaps():
            # filter consumptions and maximeter, and look for gaps
            self._data["consumptions"], miss_cons = find_gaps(
                self._data["consumptions"],
                max([date_from, _supply["start"]]),
                date_to,
                gap_interval=timedelta(hours=24),
            )
            self._data["maximeter"], miss_maxim = find_gaps(
                self._data["maximeter"],
                max([date_from, _supply["start"]]),
                date_to,
                gap_interval=timedelta(days=30),
            )

            _start_contracts = datetime.today()
            for _contract in self._data["contracts"]:
                # register oldest contract
                if _contract["start"] < _start_contracts:
                    _start_contracts = _contract["start"]

                # update consumptions during contract range
                for gap in [
                    x
                    for x in miss_cons
                    if not (
                        x["to"] < _contract["start"] or x["from"] > _contract["end"]
                    )
                ]:
                    start = max([gap["from"], _contract["start"]])
                    end = min([gap["to"], _contract["end"]])
                    self._update_consumptions(
                        cups,
                        _supply["distributor_code"],
                        start,
                        end,
                        "0",
                        _supply["point_type"],
                    )

                # update maximeter during contract range
                for gap in [
                    x
                    for x in miss_maxim
                    if not (
                        x["to"] < _contract["start"] or x["from"] > _contract["end"]
                    )
                ]:
                    start = max(
                        [gap["from"], _contract["start"] + relativedelta(months=1)]
                    )
                    end = min([gap["to"], _contract["end"]])
                    self._update_maximeter(
                        cups, _supply["distributor_code"], start, end
                    )

            # safe check periods in wrongly-registered contracts, this might throw errors
            if _supply["start"] < date_from < _start_contracts:
                self._update_consumptions(
                    cups,
                    _supply["distributor_code"],
                    max([gap["from"], _contract["start"]]),
                    _start_contracts,
                    "0",
                    _supply["point_type"],
                )
                self._update_maximeter(
                    cups, _supply["distributor_code"], date_from, _start_contracts
                )

        fill_gaps()
        # filter consumptions and maximeter, and look for gaps
        self._data["consumptions"], miss_cons = find_gaps(
            self._data["consumptions"],
            max([date_from, _supply["start"]]),
            date_to,
            gap_interval=timedelta(hours=24),
        )
        self._data["maximeter"], miss_maxim = find_gaps(
            self._data["maximeter"],
            max([date_from, _supply["start"]]),
            date_to,
            gap_interval=timedelta(days=31),
        )
        # check if some gaps were not satisfied, and retry
        if len(miss_cons) > 1 or len(miss_maxim) > 1:
            fill_gaps()
            if len(miss_cons) > 1:
                _LOGGER.warning(
                    f"{self._LABEL}: still missing the following consumption ranges {miss_cons}, will try again later"
                )
            if len(miss_maxim) > 1:
                _LOGGER.warning(
                    f"{self._LABEL}: still missing the following maximeter ranges {miss_maxim}, will try again later"
                )

        return True

    def _update_supplies(self):
        if datetime.today().date() != self._last_updated["supplies"].date():
            try:
                self._data["supplies"] = self.get_supplies()
                self._last_updated["supplies"] = datetime.now()
                return
            except (ServerError, TimeoutError) as e:
                if len(self._data["supplies"]) > 0:
                    _LOGGER.exception(
                        "got an error while updating supplies %s:%s",
                        e.source,
                        e.message,
                    )
                else:
                    raise e

    def _update_contracts(self, cups, distributor_code, authorizedNif=None):
        if datetime.today().date() != self._last_updated["contracts"].date():
            try:
                self._data["contracts"] = self.get_contract_detail(
                    cups, distributor_code
                )
                self._last_updated["contracts"] = datetime.now()
            except (ServerError, TimeoutError) as e:
                if len(self._data["contracts"]) > 0:
                    _LOGGER.exception(
                        "got an error while updating contracts %s:%s",
                        e.source,
                        e.message,
                    )
                else:
                    raise e

    def _update_consumptions(
        self,
        cups,
        distributor_code,
        startDate,
        endDate,
        measurementType,
        point_type,
        authorizedNif=None,
    ):
        try:
            self._data["consumptions"] = add_or_update(
                self._data["consumptions"],
                self.get_consumption_data(
                    cups,
                    distributor_code,
                    startDate,
                    endDate,
                    measurementType,
                    point_type,
                    authorizedNif,
                ),
                "start",
            )
            self._last_updated["consumptions"] = datetime.now()
        except (ServerError, TimeoutError) as e:
            _LOGGER.exception("%s:%s", e.source, e.message)

    def _update_maximeter(
        self, cups, distributor_code, startDate, endDate, authorizedNif=None
    ):
        try:
            self._data["maximeter"] = add_or_update(
                self._data["maximeter"],
                self.get_max_power(cups, distributor_code, startDate, endDate),
                "start",
            )
            self._last_updated["maximeter"] = datetime.now()
        except (ServerError, TimeoutError) as e:
            _LOGGER.exception("%s:%s", e.source, e.message)

    def get_supplies(self, authorizedNif=None):
        data = {}
        if authorizedNif is not None:
            data["authorizedNif"] = authorizedNif
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
                c.append(
                    Supply(
                        cups=i["cups"],
                        start=datetime.strptime(
                            i["validDateFrom"]
                            if i["validDateFrom"] != ""
                            else "1970/01/01",
                            "%Y/%m/%d",
                        ),
                        end=datetime.strptime(
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
                        point_type=i["pointType"],
                        distributor_code=i["distributorCode"],
                    )
                )
            else:
                _LOGGER.warning(
                    f"{self._LABEL}: weird data structure while fetching supplies data, got {r}"
                )
        return c

    def get_contract_detail(self, cups, distributor_code, authorizedNif=None):
        data = {"cups": cups, "distributorCode": str(distributor_code)}
        if authorizedNif is not None:
            data["authorizedNif"] = authorizedNif
        r = self._send_cmd(
            "https://datadis.es/api-private/api/get-contract-detail", data=data
        )
        c = []
        tomorrow_str = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d")
        for i in r:
            if all(
                k in i
                for k in (
                    "cups",
                    "startDate",
                    "endDate",
                    "marketer",
                    "contractedPowerkW",
                )
            ):
                _date_template = "%Y/%m/%d"
                c.append(
                    Contract(
                        cups=i["cups"],
                        start=datetime.strptime(
                            i["startDate"] if i["startDate"] != "" else "1970/01/01",
                            _date_template,
                        ),
                        end=datetime.strptime(
                            i["endDate"] if i["endDate"] != "" else tomorrow_str,
                            _date_template,
                        ),
                        marketer=i["marketer"],
                        distributor_code=distributor_code,
                        power_p1=i["contractedPowerkW"][0]
                        if isinstance(i["contractedPowerkW"], list)
                        else None,
                        power_p2=i["contractedPowerkW"][1]
                        if (len(i["contractedPowerkW"]) > 1)
                        else None,
                        power_p3=i["contractedPowerkW"][2]
                        if (len(i["contractedPowerkW"]) > 2)
                        else None,
                        power_p4=i["contractedPowerkW"][3]
                        if (len(i["contractedPowerkW"]) > 3)
                        else None,
                        power_p5=i["contractedPowerkW"][4]
                        if (len(i["contractedPowerkW"]) > 4)
                        else None,
                        power_p6=i["contractedPowerkW"][5]
                        if (len(i["contractedPowerkW"]) > 5)
                        else None,
                    )
                )
            else:
                _LOGGER.warning(
                    f"{self._LABEL}: weird data structure while fetching contracts data, got {r}"
                )
        return c

    def get_consumption_data(
        self,
        cups,
        distributor_code,
        startDate,
        endDate,
        measurementType,
        point_type,
        authorizedNif=None,
    ):
        # _LOGGER.info (f"{self._LABEL}: fetching consumptions from {startDate} to {endDate}")
        data = {
            "cups": cups,
            "distributorCode": distributor_code,
            "startDate": datetime.strftime(startDate, "%Y/%m/%d"),
            "endDate": datetime.strftime(endDate, "%Y/%m/%d"),
            "measurementType": measurementType,
            "pointType": point_type,
        }
        if authorizedNif is not None:
            data["authorizedNif"] = authorizedNif
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
                    _start = datetime.strptime(
                        f"{i['date']} {hour.zfill(2)}:00", "%Y/%m/%d %H:%M"
                    )
                    c.append(
                        Consumption(
                            start=_start,
                            end=_start + timedelta(hours=1),
                            value_kwh=i["consumptionKWh"],
                            real=True if i["obtainMethod"] == "Real" else False,
                        )
                    )
                else:
                    _LOGGER.warning(
                        f"{self._LABEL}: weird data structure while fetching consumption data, got {r}"
                    )
        return c

    def get_max_power(
        self, cups, distributor_code, startDate, endDate, authorizedNif=None
    ):
        # _LOGGER.info (f"{self._LABEL}: fetching maximeter from {startDate} to {endDate}")
        data = {
            "cups": cups,
            "distributorCode": distributor_code,
            "startDate": datetime.strftime(startDate, "%Y/%m"),
            "endDate": datetime.strftime(endDate, "%Y/%m"),
        }
        if authorizedNif is not None:
            data["authorizedNif"] = authorizedNif
        r = self._send_cmd(
            "https://datadis.es/api-private/api/get-max-power", data=data
        )
        c = []
        for i in r:
            if all(k in i for k in ("time", "date", "maxPower")):
                _time = datetime.strptime(f"{i['date']} {i['time']}", "%Y/%m/%d %H:%M")
                c.append(
                    MaxPower(
                        start=_time.replace(day=1, hour=0, minute=0),
                        end=_time.replace(day=1, hour=0, minute=0)
                        + relativedelta(months=1),
                        time=_time,
                        value_kw=i["maxPower"],
                    )
                )
            else:
                _LOGGER.warning(
                    f"{self._LABEL}: weird data structure while fetching maximeter data, got {r}"
                )
        return c


class EsiosConnector(Connector):
    UPDATE_INTERVAL = timedelta(hours=24)
    _LABEL = "EsiosConnector"

    def __init__(self, local_timezone="Europe/Madrid"):
        super().__init__()
        logging.getLogger("aiopvpc").setLevel(logging.ERROR)
        self._last_try = datetime(1970, 1, 1)
        self._local_timezone = local_timezone
        self._handler = PVPCData(
            tariff=TARIFFS[0], local_timezone=tz.timezone(self._local_timezone)
        )
        self._data = EsiosData(energy_costs=[])
        self._last_updated = {x: datetime(1970, 1, 1) for x in self._data}

    def update(self, date_from=None, date_to=None):
        # default values are to fetch full history
        date_from = datetime(1970, 1, 1) if date_from is None else date_from
        date_to = datetime.today() if date_to is None else date_to
        if (datetime.now() - self._last_try) > self.UPDATE_INTERVAL:
            try:
                self._data["energy_costs"], missing = find_gaps(
                    self._data["energy_costs"],
                    date_from,
                    date_to,
                    gap_interval=timedelta(hours=1),
                )
                for gap in missing:
                    raw = self._handler.download_prices_for_range(
                        gap["from"], gap["to"]
                    )
                    pvpc = [
                        Cost(
                            start=datetime.strptime(
                                x.astimezone(
                                    tz.timezone(self._local_timezone)
                                ).strftime("%Y-%m-%d %H:%M"),
                                "%Y-%m-%d %H:%M",
                            ),
                            end=datetime.strptime(
                                x.astimezone(
                                    tz.timezone(self._local_timezone)
                                ).strftime("%Y-%m-%d %H:%M"),
                                "%Y-%m-%d %H:%M",
                            )
                            + timedelta(hours=1),
                            value_eur=raw[x],
                        )
                        for x in raw
                    ]
                    self._data["energy_costs"] = add_or_update(
                        self._data["energy_costs"], pvpc, "start"
                    )
            except Exception as e:
                raise ServerError(
                    self._LABEL, "unhandled error when fetching pvpc data %s", e
                )
