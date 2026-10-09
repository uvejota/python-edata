"""A REData API connector"""

import asyncio
import contextlib
import datetime as dt
import logging
import typing

import aiohttp
from dateutil import parser

from edata.models import EnergyPrice

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
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        """Init method for REDataConnector

        ``session`` lets the caller share a long-lived aiohttp session; without
        it a short-lived session is opened per request.
        """
        self._session = session

    @contextlib.asynccontextmanager
    async def _client(self) -> typing.AsyncIterator[aiohttp.ClientSession]:
        """Yield the shared session, or a short-lived one when none was given."""
        if self._session is not None:
            yield self._session
        else:
            async with aiohttp.ClientSession() as session:
                yield session

    async def async_get_realtime_prices(
        self, dt_from: dt.datetime, dt_to: dt.datetime, is_ceuta_melilla: bool = False
    ) -> list[EnergyPrice]:
        """GET query to fetch realtime pvpc prices, historical data is limited to current month (async)"""
        url = URL_REALTIME_PRICES.format(
            geo_id=8744 if is_ceuta_melilla else 8741,
            start=dt_from,
            end=dt_to,
        )
        data = []
        _LOGGER.info("GET %s", url)
        timeout = aiohttp.ClientTimeout(total=REQUESTS_TIMEOUT)
        async with self._client() as session:
            try:
                async with session.get(url, timeout=timeout) as res:
                    if res.status == 200:
                        try:
                            res_json = await res.json()
                            res_list = res_json["included"][0]["attributes"]["values"]
                        except (IndexError, KeyError):
                            _LOGGER.error(
                                "%s returned a malformed response: %s ",
                                url,
                                await res.text(),
                            )
                            return data
                        for element in res_list:
                            data.append(
                                EnergyPrice(
                                    datetime=parser.parse(element["datetime"]).replace(
                                        tzinfo=None
                                    ),
                                    value_eur_kwh=element["value"] / 1000,
                                    delta_h=1,
                                )
                            )
                    else:
                        _LOGGER.error(
                            "%s returned %s with code %s",
                            url,
                            await res.text(),
                            res.status,
                        )
            except Exception as e:
                _LOGGER.error("Exception fetching realtime prices: %s", e)
        return data

    def get_realtime_prices(
        self, dt_from: dt.datetime, dt_to: dt.datetime, is_ceuta_melilla: bool = False
    ) -> list[EnergyPrice]:
        """GET query to fetch realtime pvpc prices, historical data is limited to current month (sync wrapper)"""
        return asyncio.run(
            self.async_get_realtime_prices(dt_from, dt_to, is_ceuta_melilla)
        )
