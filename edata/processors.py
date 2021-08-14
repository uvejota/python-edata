import pandas as pd
import logging

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

LIST_P1 = ['10:00', '11:00', '12:00', '13:00', '18:00', '19:00', '20:00', '21:00']
LIST_P2 = ['08:00', '09:00', '14:00', '15:00', '16:00', '17:00', '22:00', '23:00']
LIST_P3 = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00','06:00', '07:00']

DAYS_P3 = ['Saturday', 'Sunday']

class ConsumptionProcessor:
    def __init__ (self, lst):
        self.data = {}
        _df = pd.DataFrame(lst)
        if 'datetime' in _df and 'value_kWh' in _df:
            _df['datetime'] = pd.to_datetime(_df['datetime'])
            _df['weekday'] = _df['datetime'].dt.day_name()
            self.df = _df
            self.valid_data = True
        else:
            self.valid_data = False
            _LOGGER.warning ('consumptions data structure is not valid')

    def get_stats (self, dt_from, dt_to):
        if self.valid_data:
            _df = self.df.copy ()
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))]
            self.data = {
                'total_kWh': _t['value_kWh'].sum(),
                'days': _t['value_kWh'].count() / 24.0
            }
            self.data['daily_kWh'] = self.data['total_kWh'] / self.data['days'] if self.data['days'] > 0 else self.data['total_kWh']
            self.data['p1_kWh'] = _t['value_kWh'].loc[(_t['datetime'].dt.strftime('%H:%M').isin(LIST_P1)) & (~_t['weekday'].isin(DAYS_P3))].sum()
            self.data['p2_kWh'] = _t['value_kWh'].loc[(_t['datetime'].dt.strftime('%H:%M').isin(LIST_P2)) & (~_t['weekday'].isin(DAYS_P3))].sum()
            self.data['p3_kWh'] = self.data['total_kWh'] - self.data['p1_kWh'] - self.data['p2_kWh']
        return self.data

class MaximeterProcessor:
    def __init__(self, lst) -> None:
        self.data = {}
        _df = pd.DataFrame(lst)
        if 'datetime' in _df and 'value_kW' in _df:
            _df['datetime'] = pd.to_datetime(_df['datetime'])
            self.df = _df
            self.valid_data = True
        else:
            self.valid_data = False
            _LOGGER.warning ('maximeter data structure is not valid')

    def get_stats (self, dt_from, dt_to):
        if self.valid_data:
            _df = self.df.copy ()
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))]
            _t = _t.reset_index ()
            idx = _t['value_kW'].argmax ()
            self.data = {
                'peak_kW': _t['value_kW'][idx],
                'peak_date': f"{_t['datetime'][idx]}",
                'peak_mean_kWh': _t['value_kW'].mean (),
                'peak_tile90_kWh': _t['value_kW'].quantile (0.9)
            }
        return self.data