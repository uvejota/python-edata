import pandas as pd

LIST_P1 = ['10:00', '11:00', '12:00', '13:00', '18:00', '19:00', '20:00', '21:00']
LIST_P2 = ['08:00', '09:00', '14:00', '15:00', '16:00', '17:00', '22:00', '23:00']
LIST_P3 = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00','06:00', '07:00']

DAYS_P3 = ['Saturday', 'Sunday']

class ConsumptionProcessor:
    def __init__ (self, lst):
        _df = pd.DataFrame(lst)
        _df['datetime'] = pd.to_datetime(_df['datetime'])
        _df['weekday'] = _df['datetime'].dt.day_name()
        self.df = _df

    def get_stats (self, dt_from, dt_to):
        _df = self.df.copy ()
        _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))]
        data = {
            'total_kWh': _t['value_kWh'].sum(),
            'days': _t['value_kWh'].count() / 24.0
        }
        data['daily_kWh'] = data['total_kWh'] / data['days'] if data['days'] > 0 else data['total_kWh']
        data['p1_kWh'] = _t['value_kWh'].loc[(_t['datetime'].dt.strftime('%H:%M').isin(LIST_P1)) & (~_t['weekday'].isin(DAYS_P3))].sum()
        data['p2_kWh'] = _t['value_kWh'].loc[(_t['datetime'].dt.strftime('%H:%M').isin(LIST_P2)) & (~_t['weekday'].isin(DAYS_P3))].sum()
        data['p3_kWh'] = data['total_kWh'] - data['p1_kWh'] - data['p2_kWh']
        return data

class MaximeterProcessor:
    def __init__(self, lst) -> None:
        _df = pd.DataFrame(lst)
        _df['datetime'] = pd.to_datetime(_df['datetime'])
        self.df = _df

    def get_stats (self, dt_from, dt_to):
        _df = self.df.copy ()
        _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))]
        _t = _t.reset_index ()
        idx = _t['value_kW'].argmax ()
        data = {
            'peak_kW': _t['value_kW'][idx],
            'peak_date': f"{_t['datetime'][idx]}",
            'peak_mean_kWh': _t['value_kW'].mean (),
            'peak_tile90_kWh': _t['value_kW'].quantile (0.9)
        }
        return data