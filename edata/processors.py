import pandas as pd
import logging
from datetime import datetime

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HOURS_P1 = [10, 11, 12, 13, 18, 19, 20, 21]
HOURS_P2 = [8, 9, 14, 15, 16, 17, 22, 23]
WEEKDAYS_P3 = [5, 6]
LIST_P1 = ['10:00', '11:00', '12:00', '13:00', '18:00', '19:00', '20:00', '21:00']
LIST_P2 = ['08:00', '09:00', '14:00', '15:00', '16:00', '17:00', '22:00', '23:00']
LIST_P3 = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00','06:00', '07:00']

DAYS_P3 = ['Saturday', 'Sunday']

class ConsumptionProcessor:
    
    def __init__ (self, lst):
        self.stats = {}
        if len(lst) > 0:
            self.df = self.preprocess (pd.DataFrame(lst))
        else:
            self.valid_data = False

    def preprocess (self, df):

        def get_px (dt):
            hour = dt.hour
            weekday = dt.weekday()
            if weekday in WEEKDAYS_P3:
                return 'p3'
            elif hour in HOURS_P1:
                return 'p1'
            elif hour in HOURS_P2:
                return 'p2'
            else:
                return 'p3'

        if 'datetime' in df and 'value_kWh' in df:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['weekday'] = df['datetime'].dt.day_name()
            df['px'] = df['datetime'].apply (get_px)
            self.valid_data = True
        else:
            self.valid_data = False
            _LOGGER.warning ('consumptions data structure is not valid')

        return df

    def group_by (self, dt_from=datetime(1970, 1, 1), dt_to=datetime.now(), key='M'):

        if key == 'M':
            date_format = '%Y-%m'
        elif key == 'D':
            date_format = '%Y-%m-%d'
        else:
            _LOGGER.error ('wrong group_by key parameter')
            return

        if self.valid_data:
            _df = self.df.copy ()
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))]
            
            for p in ['p1', 'p2', 'p3']:
                _t['value_'+p+'_kWh'] = _t['value_kWh'].where(_t['px']==p)
            _t.drop (['real'], axis=1, inplace=True)
            _t = _t.groupby ([_t.datetime.dt.to_period(key)]).sum ()
            _t.reset_index (inplace=True)
            
            _t['datetime'] = _t['datetime'].dt.strftime(date_format)
            _t = _t.round(2)
            raw_dict = _t.to_dict('records')
            return raw_dict

    def get_stats (self, dt_from=datetime(1970, 1, 1), dt_to=datetime.now()):
        if self.valid_data:
            _df = self.df.copy ()
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))]
            self.stats = {
                'total_kWh': _t['value_kWh'].sum(),
                'days': _t['value_kWh'].count() / 24.0
            }
            self.stats['daily_kWh'] = self.stats['total_kWh'] / self.stats['days'] if self.stats['days'] > 0 else self.stats['total_kWh']
            self.stats['p1_kWh'] = _t['value_kWh'][_t['px']=='p1'].sum()
            self.stats['p2_kWh'] = _t['value_kWh'][_t['px']=='p2'].sum()
            self.stats['p3_kWh'] = _t['value_kWh'][_t['px']=='p3'].sum()
        return self.stats

class MaximeterProcessor:
    def __init__(self, lst) -> None:
        self.stats = {}
        _df = pd.DataFrame(lst)
        if len(lst) > 0:
            if 'datetime' in _df and 'value_kW' in _df:
                _df['datetime'] = pd.to_datetime(_df['datetime'])
                self.df = _df
                self.valid_data = True
            else:
                self.valid_data = False
                _LOGGER.warning ('maximeter data structure is not valid')
        else:
            self.valid_data = False

    def get_stats (self, dt_from, dt_to):
        if self.valid_data:
            _df = self.df.copy ()
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))]
            _t = _t.reset_index ()
            idx = _t['value_kW'].argmax ()
            self.stats = {
                'peak_kW': _t['value_kW'][idx],
                'peak_date': f"{_t['datetime'][idx]}",
                'peak_mean_kWh': _t['value_kW'].mean (),
                'peak_tile90_kWh': _t['value_kW'].quantile (0.9)
            }
        return self.stats
