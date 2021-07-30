import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from edata.connectors import *
from edata.processors import *
import asyncio

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

PLATFORMS = ['datadis', 'edistribucion']
ATTRIBUTES = {
    "cups": None,
    "energy_total": 'kWh',
    "icp_status": None,
    "power_load": '%',
    "power_limit_p1": 'kW',
    "power_limit_p2": 'kW',
    "power": 'kW',
    "energy_today": 'kWh',
    "energy_today_p1": 'kWh',
    "energy_today_p2": 'kWh',
    "energy_today_p3": 'kWh',
    "energy_yesterday": 'kWh',
    "energy_yesterday_p1": 'kWh',
    "energy_yesterday_p2": 'kWh',
    "energy_yesterday_p3": 'kWh',
    "cycle_current": 'kWh',
    "cycle_current_daily": 'kWh',
    "cycle_current_days": 'd',
    "cycle_current_p1": 'kWh',
    "cycle_current_p2": 'kWh',
    "cycle_current_p3": 'kWh',
    "cycle_current_pvpc": '€',
    "cycle_last": 'kWh',
    "cycle_last_daily": 'kWh',
    "cycle_last_days": 'd',
    "cycle_last_p1": 'kWh',
    "cycle_last_p2": 'kWh',
    "cycle_last_p3": 'kWh',
    "cycle_last_pvpc": '€',
    "power_peak": 'kW',
    "power_peak_date": None,
    "power_peak_mean": 'kW',
    "power_peak_tile90": 'kW'
}

LIST_P1 = ['11:00', '12:00', '13:00', '14:00', '19:00', '20:00', '21:00', '22:00']
LIST_P2 = ['09:00', '10:00', '15:00', '16:00', '17:00', '18:00', '23:00', '24:00']
LIST_P3 = ['01:00', '02:00', '03:00', '04:00', '05:00', '06:00','07:00', '08:00']

DAYS_P3 = ['Saturday', 'Sunday']

class PlatformError(Exception):

    def __init__(self, where, message):
        self.where = where
        self.message = message

    def __str__(self) -> str:
        return f'at {self.where}: {self.message}'

class Edata ():

    data = {}
    attr = {}

    def __init__(self, platform, username, password, cups) -> None:
        self.__usr = username
        self.__pwd = password
        self.__cups = cups
        self.__loop = None
        if platform == 'datadis':
            self.__conn = DatadisConnector (username, password)
        elif platform == 'edistribucion':
            self.__conn = EdistribucionConnector (username, password)
        else:
            raise PlatformError ('', f'platform {platform} not supported, valid options are {PLATFORMS}')

    def async_update (self, date_from=datetime(1970, 1, 1), date_to=datetime.today()):
        if self.__loop is None:
            self.__loop = asyncio.get_event_loop()
        self.__loop.run_in_executor(None, self.update, [date_from, date_to])

    def update (self, date_from=datetime(1970, 1, 1), date_to=datetime.today()):
        if (self.update_data (self.__cups, date_from, date_to)):
            self.update_attr ()

    def update_data (self, cups, date_from=None, date_to=None):
        changed = False
        self.__conn.update (cups, date_from, date_to)
        if self.__conn.data is not None and self.data != self.__conn.data:
            self.data = self.__conn.data
            changed = True
        return changed

    def update_attr (self):

        self.update_attr_supplies ()
        self.update_attr_contracts ()
        self.update_attr_consumptions ()
        self.update_attr_maximeter ()
        self.update_attr_meter ()

        for a in self.attr:
            if a in ATTRIBUTES and ATTRIBUTES[a] is not None:
                self.attr[a] = round(self.attr[a], 2) if self.attr[a] is not None else '-'

    def update_attr_supplies (self):
        for i in self.data['supplies']:
            if i['cups'] == self.__cups:
                self.attr['cups'] = self.__cups
                break

    def update_attr_contracts (self):
        for i in self.data['contracts']:
            if i['date_end'] is None:
                self.attr['power_limit_p1'] = i['power_p1']
                self.attr['power_limit_p2'] = i['power_p2']
                break

    def update_attr_consumptions (self):

        processor = ConsumptionProcessor (self.data['consumptions'])

        today_starts = datetime (
            datetime.today ().year, 
            datetime.today ().month, 
            datetime.today ().day, 0, 0, 0
        )
        
        # update today
        a = processor.get_stats (today_starts, today_starts + timedelta (days=1))
        self.attr["energy_today"] = a['total_kWh']
        self.attr["energy_today_p1"] = a['p1_kWh']
        self.attr["energy_today_p2"] = a['p2_kWh']
        self.attr["energy_today_p3"] = a['p3_kWh']

        # update yesterday
        a = processor.get_stats (today_starts-timedelta(days=1), today_starts)
        self.attr["energy_yesterday"] = a['total_kWh']
        self.attr["energy_yesterday_p1"] = a['p1_kWh']
        self.attr["energy_yesterday_p2"] = a['p2_kWh']
        self.attr["energy_yesterday_p3"] = a['p3_kWh']

        cycle_starts = datetime (
            datetime.today ().year, 
            datetime.today ().month, 1, 0, 0, 0
        )

        # update current cycle
        a = processor.get_stats (cycle_starts, cycle_starts + relativedelta(months=1))
        self.attr["cycle_current"] = a['total_kWh']
        self.attr["cycle_current_days"] = a['days']
        self.attr["cycle_current_daily"] = a['daily_kWh']
        self.attr["cycle_current_p1"] = a['p1_kWh']
        self.attr["cycle_current_p2"] = a['p2_kWh']
        self.attr["cycle_current_p3"] = a['p3_kWh']

        # update last cycle
        a = processor.get_stats (cycle_starts - relativedelta (months=1), cycle_starts)
        self.attr["cycle_last"] = a['total_kWh']
        self.attr["cycle_last_days"] = a['days']
        self.attr["cycle_last_daily"] = a['daily_kWh']
        self.attr["cycle_last_p1"] = a['p1_kWh']
        self.attr["cycle_last_p2"] = a['p2_kWh']
        self.attr["cycle_last_p3"] = a['p3_kWh']

    def update_attr_maximeter (self):
        processor = MaximeterProcessor (self.data['maximeter'])
        a = processor.get_stats (datetime.today() - timedelta(days=365), datetime.today())
        self.attr['power_peak'] = a['peak_kW']
        self.attr['power_peak_date'] = a['peak_date']
        self.attr['power_peak_mean'] = a['peak_mean_kWh']
        self.attr['power_peak_tile90'] = a['peak_tile90_kWh']

    def update_attr_meter (self):
        try:
            meter = self.data['meter']
            self.attr["energy_total"] = meter['energy_kwh']
            self.attr["icp_status"] = meter['icp_status']
            self.attr["power_load"] = meter['power_%']
            self.attr["power"] = meter['power_kw']
        except KeyError as e:
            pass

    def __str__(self) -> str:
        return '\n'.join([f'{i}: {self.attr[i]}' for i in self.attr])