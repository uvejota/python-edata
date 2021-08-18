import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from edata.connectors import *
from edata.processors import *
import asyncio

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ['datadis']
ATTRIBUTES = {
    "cups": None,
    "contract_p1_kW": 'kW',
    "contract_p2_kW": 'kW',
    "yesterday_kWh": 'kWh',
    "yesterday_p1_kWh": 'kWh',
    "yesterday_p2_kWh": 'kWh',
    "yesterday_p3_kWh": 'kWh',
    "month_kWh": 'kWh',
    "month_daily_kWh": 'kWh',
    "month_days": 'd',
    "month_p1_kWh": 'kWh',
    "month_p2_kWh": 'kWh',
    "month_p3_kWh": 'kWh',
    "last_month_kWh": 'kWh',
    "last_month_daily_kWh": 'kWh',
    "last_month_days_kWh": 'd',
    "last_month_p1_kWh": 'kWh',
    "last_month_p2_kWh": 'kWh',
    "last_month_p3_kWh": 'kWh',
    "max_power_kW": 'kW',
    "max_power_date": None,
    "max_power_mean_kW": 'kW',
    "max_power_90perc_kW": 'kW'
}

class PlatformError(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self) -> str:
        return f'{self.message}'

class ReportHelper ():

    data = {}
    attributes = {}

    def __init__(self, platform, username, password, cups, log_level=logging.WARNING) -> None:
        self._cups = cups
        self._loop = None
        logging.basicConfig(level=log_level)

        if platform == 'datadis':
            self._conn = DatadisConnector (username, password, log_level=log_level)
        else:
            raise PlatformError (f'platform {platform} not supported, valid options are {PLATFORMS}')

        for x in ATTRIBUTES:
            self.attributes[x] = None

    async def async_update (self):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        self._loop.run_in_executor(None, self.update)

    def update (self):
        date_from = datetime (
                datetime.today ().year, 
                datetime.today ().month, 
                1, 0, 0, 0
            ) - relativedelta (months=12)
        date_to = datetime.today()
        if self.update_data (self._cups, date_from, date_to):
            self.data = self._conn.data
            self.update_attributes ()

    def update_data (self, cups, date_from=None, date_to=None):
        updated = False
        try:
            updated = self._conn.update (cups, date_from, date_to)
        except Exception as e:
            _LOGGER.error (f"unhandled exception while updating data for CUPS {cups[-4:]}")
            _LOGGER.exception (e)
        return updated

    def update_attributes (self):
        for f in [self.update_attr_supplies, self.update_attr_contracts, self.update_attr_consumptions, self.update_attr_maximeter]:
            try:
                f()
            except Exception as e:
                _LOGGER.error (f"unhandled exception while updating attributes")
                _LOGGER.exception (e)

        for a in self.attributes:
            if a in ATTRIBUTES and ATTRIBUTES[a] is not None:
                self.attributes[a] = round(self.attributes[a], 2) if self.attributes[a] is not None else None

    def update_attr_supplies (self):
        for i in self.data['supplies']:
            if i['cups'] == self._cups:
                self.attributes['cups'] = self._cups
                break

    def update_attr_contracts (self):
        most_recent_date = datetime (1970, 1, 1)
        for i in self.data['contracts']:
            if i['date_end'] > most_recent_date:
                most_recent_date = i['date_end']
                self.attributes['contract_p1_kW'] = i.get('power_p1', None)
                self.attributes['contract_p2_kW'] = i.get('power_p2', None)
                break

    def update_attr_consumptions (self):        
        processor = ConsumptionProcessor (self.data['consumptions'])

        today_starts = datetime (
            datetime.today ().year, 
            datetime.today ().month, 
            datetime.today ().day, 0, 0, 0
        )
        
        # update yesterday
        a = processor.get_stats (today_starts-timedelta(days=1), today_starts)
        self.attributes["yesterday_kWh"] = a.get('total_kWh', None)
        self.attributes["yesterday_p1_kWh"] = a.get('p1_kWh', None)
        self.attributes["yesterday_p2_kWh"] = a.get('p2_kWh', None)
        self.attributes["yesterday_p3_kWh"] = a.get('p3_kWh', None)

        cycle_starts = datetime (
            datetime.today ().year, 
            datetime.today ().month, 1, 0, 0, 0
        )

        # update current cycle
        a = processor.get_stats (cycle_starts, cycle_starts + relativedelta(months=1))
        self.attributes["month_kWh"] = a.get('total_kWh', None)
        self.attributes["month_days"] = a.get('days', None)
        self.attributes["month_daily_kWh"] = a.get('daily_kWh', None)
        self.attributes["month_p1_kWh"] = a.get('p1_kWh', None)
        self.attributes["month_p2_kWh"] = a.get('p2_kWh', None)
        self.attributes["month_p3_kWh"] = a.get('p3_kWh', None)

        # update last cycle
        a = processor.get_stats (cycle_starts - relativedelta (months=1), cycle_starts)
        self.attributes["last_month_kWh"] = a.get('total_kWh', None)
        self.attributes["last_month_days_kWh"] = a.get('days', None)
        self.attributes["last_month_daily_kWh"] = a.get('daily_kWh', None)
        self.attributes["last_month_p1_kWh"] = a.get('p1_kWh', None)
        self.attributes["last_month_p2_kWh"] = a.get('p2_kWh', None)
        self.attributes["last_month_p3_kWh"] = a.get('p3_kWh', None)

    def update_attr_maximeter (self):

        date_from = datetime (
                datetime.today ().year, 
                datetime.today ().month, 
                1, 0, 0, 0
            ) - relativedelta (months=12)
        processor = MaximeterProcessor (self.data['maximeter'])
        a = processor.get_stats (date_from, datetime.today())
        self.attributes['max_power_kW'] = a.get('peak_kW', None)
        self.attributes['max_power_date'] = a.get('peak_date', None)
        self.attributes['max_power_mean_kW'] = a.get('peak_mean_kWh', None)
        self.attributes['max_power_90perc_kW'] = a.get('peak_tile90_kWh', None)

    def __str__(self) -> str:
        return '\n'.join([f'{i}: {self.attributes[i]}' for i in self.attributes])