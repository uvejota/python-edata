import pandas as pd
import logging
from datetime import datetime, timedelta

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HOURS_P1 = [10, 11, 12, 13, 18, 19, 20, 21]
HOURS_P2 = [8, 9, 14, 15, 16, 17, 22, 23]
WEEKDAYS_P3 = [5, 6]

class CommonProcessor:
    """A collection of static methods to process datasets"""
    
    @staticmethod
    def is_empty (lst):
        return len(lst) == 0

    @staticmethod
    def extract_dt_range (lst, dt_from, dt_to):
        df = pd.DataFrame(lst)
        return df.loc[(pd.to_datetime(dt_from) <= df['datetime']) & (df['datetime'] < pd.to_datetime(dt_to))].to_dict('records')

    @staticmethod
    def export_as_csv (lst, dest_file):
        df = pd.DataFrame(lst)
        df.to_csv(dest_file)
        

class ConsumptionProcessor:
    _LABEL = 'ConsumptionProcessor'
    
    def __init__ (self, lst):
        self.df = self.preprocess (pd.DataFrame(lst))

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
            _LOGGER.warning (f'{self._LABEL} wrong data structure')

        return df

    def group_by (self, dt_from=datetime(1970, 1, 1), dt_to=datetime.now(), key='M', action='sum'):

        if key == 'M':
            date_format = '%Y-%m'
        elif key == 'D':
            date_format = '%Y-%m-%d'
        else:
            _LOGGER.error (f'{self._LABEL} {key} is not a valid group_by key parameter')
            return

        if action not in ['sum', 'mean']:
            _LOGGER.error (f'{self._LABEL} {action} is not a valid group_by action parameter')
            return

        if self.valid_data:
            _df = self.df
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))].copy ()
            
            for p in ['p1', 'p2', 'p3']:
                _t['value_'+p+'_kWh'] = _t.loc[_t['px']==p,'value_kWh']
            _t.drop (['real'], axis=1, inplace=True)
            if action == 'sum':
                _t = _t.groupby ([_t.datetime.dt.to_period(key)]).sum ()
            elif action == 'mean':
                _t = _t.groupby ([_t.datetime.dt.to_period(key)]).mean ()
            _t.reset_index (inplace=True)
            
            _t['datetime'] = _t['datetime'].dt.strftime(date_format)
            _t = _t.round(2)
            grouped_data = _t.to_dict('records')
            return grouped_data

    def process_range (self, dt_from=datetime(1970, 1, 1), dt_to=datetime.now()):
        stats = {}
        if self.valid_data:
            _df = self.df
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))].copy ()
            stats = {
                'total_kWh': _t['value_kWh'].sum(),
                'days': _t['value_kWh'].count() / 24.0
            }
            stats['daily_kWh'] = stats['total_kWh'] / stats['days'] if stats['days'] > 0 else stats['total_kWh']
            stats['p1_kWh'] = _t['value_kWh'][_t['px']=='p1'].sum()
            stats['p2_kWh'] = _t['value_kWh'][_t['px']=='p2'].sum()
            stats['p3_kWh'] = _t['value_kWh'][_t['px']=='p3'].sum()
            stats['idle_avg_W'] = 1000*_t['value_kWh'].quantile (0.1)
        return stats

class MaximeterProcessor:
    _LABEL = 'MaximeterProcessor'

    def __init__(self, lst) -> None:
        self.df = self.preprocess (pd.DataFrame(lst))
        
    def preprocess (self, df):
        if 'datetime' in df and 'value_kW' in df:
            df['datetime'] = pd.to_datetime(df['datetime'])
            self.valid_data = True
        else:
            self.valid_data = False
            _LOGGER.warning (f'{self._LABEL} wrong data structure')
        return df

    def process_range (self, dt_from, dt_to):
        stats = {}
        if self.valid_data:
            _df = self.df
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))].copy ()
            _t = _t.reset_index ()
            idx = _t['value_kW'].argmax ()
            stats = {
                'peak_kW': _t['value_kW'][idx],
                'peak_date': f"{_t['datetime'][idx]}",
                'peak_mean_kWh': _t['value_kW'].mean (),
                'peak_tile90_kWh': _t['value_kW'].quantile (0.9)
            }
        return stats

class BillingProcessor:
    _LABEL = 'BillingProcessor'

    const = {
        'p1_kw*y': 30.67266, # €/kW/year 
        'p2_kw*y': 1.4243591, # €/kW/year
        'meter_m': 0.81, # €/month
        'market_kw*y': 3.113, # €/kW/año
        'e_tax': 1.0511300560, # multiplicative
        'iva_tax': 1.1 # multiplicative
    }

    def __init__ (self, consumptions_lst, contracts_lst, prices_lst, const={}):
        self.preprocess (consumptions_lst, contracts_lst, prices_lst)
        for i in const:
            self.const[i] = const[i]

    def preprocess (self, consumptions_lst, contracts_lst, prices_lst):
        self.valid_data = False
        c_df = pd.DataFrame (consumptions_lst)
        if all (k in c_df for k in ("datetime", "value_kWh")):
            c_df['datetime'] = pd.to_datetime(c_df['datetime'])
            df = c_df
            p_df = pd.DataFrame (prices_lst)
            if all (k in p_df for k in ("datetime", "price")):
                p_df['datetime'] = pd.to_datetime(p_df['datetime'])
                df = df.merge (p_df, how='left', left_on=['datetime'], right_on=['datetime'])
                c = []
                try:
                    for contract in contracts_lst:
                        start = contract['date_start']
                        end = contract['date_end']
                        finish = False
                        while not finish:
                            c.append (
                                {
                                    'datetime': start,
                                    'power_p1': contract['power_p1'],
                                    'power_p2': contract['power_p2'] if contract['power_p2'] is not None else contract['power_p1']
                                }
                            )
                            start = start + timedelta (hours=1)
                            finish = not (end > start)
                    df = df.merge (pd.DataFrame (c), how='left', left_on=['datetime'], right_on=['datetime'])
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df['e_taxfree'] = df['price'] * df['value_kWh'] 
                    df['e_wtax'] = df['e_taxfree'] * self.const['e_tax'] * self.const['iva_tax']
                    hprice_p1 = self.const['p1_kw*y'] / 365 / 24
                    hprice_p2 = self.const['p2_kw*y'] / 365 / 24
                    hprice_market = self.const['market_kw*y'] / 365 / 24
                    df['p_taxfree'] = df['power_p1'] * (hprice_p1 + hprice_market) + df['power_p2'] * hprice_p2 
                    df['p_wtax'] = df['p_taxfree'] * self.const['e_tax'] * self.const['iva_tax']
                    self.df = df
                    self.valid_data = True
                except Exception as e:
                    _LOGGER.warning (f'{self._LABEL} wrong contracts data structure')
                    _LOGGER.exception (e)
            else:
                _LOGGER.warning (f'{self._LABEL} wrong prices data structure')
        else:
            _LOGGER.warning (f'{self._LABEL} wrong consumptions data structure')

    def process_range (self, dt_from=datetime(1970, 1, 1), dt_to=datetime.now()):
        data = {}
        if self.valid_data:
            _df = self.df
            _t = _df.loc[(pd.to_datetime(dt_from) <= _df['datetime']) & (_df['datetime'] < pd.to_datetime(dt_to))].copy ()
            data = {
                'energy_term': round(_t['e_wtax'].sum(), 2),
                'power_term': round(_t['p_wtax'].sum(), 2),
                'other_terms': round(self.const['iva_tax'] * ((dt_to - dt_from).total_seconds() /(24*3600)) * self.const['meter_m'] / 30, 2)
            }
            data['total'] = data['energy_term'] + data['power_term'] + data['other_terms']
        return data

        
