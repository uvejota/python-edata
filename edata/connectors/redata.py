"""A REData API connector"""

import datetime as dt
import logging

import requests
from dateutil import parser

from ..definitions import PricingData

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)

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
        log_level: int = logging.WARNING,
    ) -> None:
        """Init method for REDataConnector"""
        logging.getLogger().setLevel(log_level)

    def get_realtime_prices(
        self, dt_from: dt.datetime, dt_to: dt.datetime, is_ceuta_melilla: bool = False
    ) -> list:
        """GET query to fetch realtime pvpc prices, historical data is limited to current month"""
        url = URL_REALTIME_PRICES.format(
            geo_id=8744 if is_ceuta_melilla else 8741,
            start=max(dt.datetime.today().replace(day=1, hour=0, minute=0), dt_from),
            end=min(
                (dt.datetime.today() + dt.timedelta(days=2)).replace(hour=0, minute=0),
                dt_to,
            ),
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