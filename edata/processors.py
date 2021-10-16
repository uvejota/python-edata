from abc import ABC, abstractmethod
import pandas as pd
import logging
from datetime import datetime, timedelta
from copy import deepcopy
import holidays
from .const import *
from .data import *

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HOURS_P1 = [10, 11, 12, 13, 18, 19, 20, 21]
HOURS_P2 = [8, 9, 14, 15, 16, 17, 22, 23]
WEEKDAYS_P3 = [5, 6]

class DataUtils:
    """A collection of static methods to process datasets"""
    @staticmethod
    def get_pvpc_tariff (a_datetime):
        hdays = holidays.CountryHoliday('ES')
        hour = a_datetime.hour
        weekday = a_datetime.weekday()
        if weekday in WEEKDAYS_P3 or a_datetime.date() in hdays:
            return 'p3'
        elif hour in HOURS_P1:
            return 'p1'
        elif hour in HOURS_P2:
            return 'p2'
        else:
            return 'p3'

class Processor (ABC):
    _LABEL = 'Processor'

    def __init__ (self, input, settings={}, auto=True):
        self._input = deepcopy(input)
        self._settings = settings
        self._ready = False
        if auto:
            self.do_process ()

    @abstractmethod
    def do_process (self):
        pass

    @property
    def output (self):
        return deepcopy(self._output)

class ConsumptionProcessor (Processor):
    _LABEL = 'ConsumptionProcessor'
    
    def do_process (self):
        self._output = {
            'hourly': [],
            'daily': [],
            'monthly': []
        }
        self._df = pd.DataFrame (self._input)
        if all (k in self._df for k in ("start", "value_kwh")):
            self._df["start"] = pd.to_datetime(self._df["start"])
            self._df['weekday'] = self._df["start"].dt.day_name()
            self._df['tariff'] = self._df["start"].apply (DataUtils.get_pvpc_tariff)
            self._df.drop (['real', 'end'], axis=1, inplace=True)
            for opt in [{'period': 'M', 'dictkey': 'monthly'}, {'period': 'D', 'dictkey': 'daily'}]:
                _t = self._df.copy ()
                for p in ['p1', 'p2', 'p3']:
                    _t['value_'+p+'_kwh'] = _t.loc[_t['tariff']==p,'value_kwh']
                _t = _t.groupby ([_t.start.dt.to_period(opt['period'])]).sum ()
                _t.reset_index (inplace=True)
                _t = _t.round(2)
                _t["start"] = _t["start"].dt.strftime("%Y-%m-%dT%H:%M:00Z")
                self._output[opt['dictkey']] = _t.to_dict('records')
            self._ready = True
            self._df["start"] = self._df["start"].dt.strftime("%Y-%m-%dT%H:%M:00Z")
            self._output['hourly'] = self._df.to_dict('records')
        elif len(self._df) > 0:
            _LOGGER.warning (f'{self._LABEL} wrong data structure')
            return False

class MaximeterProcessor (Processor):
    _LABEL = 'MaximeterProcessor'

    def do_process (self):
        self._output = {
            'stats': {}
        }
        self._df = pd.DataFrame (self._input)
        if all (k in self._df for k in ("start", "value_kw")):
            idx = self._df['value_kw'].argmax ()
            self._output['stats'] = {
                'value_max_kw': self._df['value_kw'][idx],
                'date_max': f"{self._df['start'][idx]}",
                'value_mean_kw': self._df['value_kw'].mean (),
                'value_tile90_kw': self._df['value_kw'].quantile (0.9)
            }            
        elif len(self._df) > 0:
            _LOGGER.warning (f'{self._LABEL} wrong data structure')
            return False

class BillingProcessor (Processor):
    _LABEL = 'BillingProcessor'

    const = {
        'p1_kw*y': 30.67266, # €/kw/year 
        'p2_kw*y': 1.4243591, # €/kw/year
        'meter_m': 0.81, # €/month
        'market_kw*y': 3.113, # €/kw/año
        'electricity_tax': 1.0511300560, # multiplicative
        'iva_tax': 1.1 # multiplicative
    }

    def do_process (self):
        self._output = {
            'hourly': [],
            'daily': [],
            'monthly': []
        }
        c_df = pd.DataFrame (self._input['consumptions'])
        if all (k in c_df for k in ("start", "value_kwh")):
            c_df["start"] = pd.to_datetime(c_df["start"])
            df = c_df
            p_df = pd.DataFrame (self._input['energy_costs'])
            if all (k in p_df for k in ("start", "value_eur")):
                p_df["start"] = pd.to_datetime(p_df["start"])
                df = df.merge (p_df, how='left', left_on=["start"], right_on=["start"])
                c = []
                try:
                    for contract in self._input['contracts']:
                        start = contract['start']
                        end = contract['end']
                        finish = False
                        while not finish:
                            c.append (
                                {
                                    "start": start,
                                    'power_p1': contract['power_p1'],
                                    'power_p2': contract['power_p2'] if contract['power_p2'] is not None else contract['power_p1']
                                }
                            )
                            start = start + timedelta (hours=1)
                            finish = not (end > start)
                    df = df.merge (pd.DataFrame (c), how='left', left_on=["start"], right_on=["start"])
                    df["start"] = pd.to_datetime(df["start"])
                    df['energy_eur'] = df['value_eur'] * df['value_kwh'] 
                    hprice_p1 = self.const['p1_kw*y'] / 365 / 24
                    hprice_p2 = self.const['p2_kw*y'] / 365 / 24
                    hprice_market = self.const['market_kw*y'] / 365 / 24
                    df['power_eur'] = df['power_p1'] * (hprice_p1 + hprice_market) + df['power_p2'] * hprice_p2 
                    df["other_eur"] = self.const["meter_m"] / 30 / 24
                    df["total_eur"] = (
                        (
                            (
                                df['power_eur'] + df['energy_eur']) * self.const['electricity_tax'] * self.const['iva_tax'] # power + energy terms
                            ) + (
                                df["other_eur"] * self.const['iva_tax'] # other (meter) terms
                                )
                    )
                    df["taxes_eur"] = df["total_eur"] - (df['energy_eur'] + df['power_eur'] + df["other_eur"])
                    self._output['hourly'] = df[["start", "total_eur", "energy_eur", "power_eur", "other_eur", "taxes_eur"]].to_dict('records')
                    for opt in [{'period': 'M', 'dictkey': 'monthly'}, {'period': 'D', 'dictkey': 'daily'}]:
                        _t = df.copy ()
                        _t = _t.groupby ([_t.start.dt.to_period(opt['period'])]).sum ()
                        _t.reset_index (inplace=True)
                        _t = _t.round(2)
                        _t["start"] = _t["start"].dt.strftime("%Y-%m-%dT%H:%M:00Z")
                        self._output[opt['dictkey']] = _t[["start", "total_eur", "energy_eur", "power_eur", "other_eur", "taxes_eur"]].to_dict('records')
                except Exception as e:
                    _LOGGER.exception (f'{self._LABEL} unhandled exception {e}')
            elif len(p_df) > 0:
                _LOGGER.warning (f'{self._LABEL} wrong prices data structure')
        elif len(c_df) > 0:
            _LOGGER.warning (f'{self._LABEL} wrong consumptions data structure')

        
