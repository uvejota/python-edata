"""A REData API connector"""

import datetime as dt
import logging
from typing import Optional

import requests
from dateutil import parser

from ..definitions import PricingData

_LOGGER = logging.getLogger(__name__)

REQUESTS_TIMEOUT = 15

URL_REALTIME_PRICES = (
    "https://apidatos.ree.es/es/datos/mercados/precios-mercados-tiempo-real"
    "?time_trunc=hour"
    "&geo_ids={geo_id}"
    "&start_date={start:%Y-%m-%dT%H:%M}&end_date={end:%Y-%m-%dT%H:%M}"
)


class REDataConnector:
    """Main class for REData connector"""

    def __init__(
        self,
    ) -> None:
        """Init method for REDataConnector"""

    def get_realtime_prices(
        self, dt_from: dt.datetime, dt_to: dt.datetime, is_ceuta_melilla: bool = False
    ) -> list:
        """GET query to fetch realtime pvpc prices, historical data is limited to current month"""
        url = URL_REALTIME_PRICES.format(
            geo_id=8744 if is_ceuta_melilla else 8741,
            start=dt_from,
            end=dt_to,
        )
        data = []
        res = requests.get(url, timeout=REQUESTS_TIMEOUT)
        if res.status_code == 200 and res.json():
            res_json = res.json()
            try:
                res_list = res_json["included"][0]["attributes"]["values"]
            except IndexError:
                _LOGGER.error(
                    "%s returned a malformed response: %s ",
                    url,
                    res.text,
                )
                return data

            for element in res_list:
                data.append(
                    PricingData(
                        datetime=parser.parse(element["datetime"]).replace(tzinfo=None),
                        value_eur_kWh=element["value"] / 1000,
                        delta_h=1,
                    )
                )
        else:
            _LOGGER.error(
                "%s returned %s with code %s",
                url,
                res.text,
                res.status_code,
            )

        return data
