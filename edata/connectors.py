from abc import ABC, abstractmethod
import requests, jwt, logging, json
from bs4 import BeautifulSoup as bs
from urllib.parse import urlparse, unquote
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.WARNING)
_LOGGER = logging.getLogger(__name__)

_LABEL = 'Connector'

class DatadisConnector ():
    UPDATE_INTERVAL = timedelta(minutes=60)
    _token = {}
    _usr = None
    _pwd = None
    _session = None

    _last_try = datetime(1970, 1, 1)

    def __init__(self, username, password):
        self._usr = username
        self._pwd = password
        self._session = requests.Session()
        self.data = {
            'supplies': [],
            'contracts': [],
            'consumptions': [],
            'maximeter': []
        }

    def _get_token (self):
        _LOGGER.info (f'{_LABEL}: no token found, fetching a new one...')
        is_valid_token = False
        self._session = requests.Session()
        credentials = {'username': self._usr, 'password': self._pwd}
        r = self._session.post("https://datadis.es/nikola-auth/tokens/login", data=credentials)
        if (r.status_code == 200):
            # store token both encoded and decoded
            self._token['encoded'] = r.text
            self._token['decoded'] = jwt.decode(self._token['encoded'], options={"verify_signature": False})
            # prepare session authorization bearer
            self._session.headers['Authorization'] = 'Bearer ' + self._token['encoded']
            _LOGGER.info (f"{_LABEL}: token received {self._token['decoded']}")
            is_valid_token = True
        else:
            _LOGGER.error (f'{_LABEL}: unknown error while retrieving token, got {r.text}')
        return is_valid_token

    def _send_cmd (self, url, data={}, refresh_token=False):
        # refresh token if needed (recursive approach)
        is_valid_token = False
        response = []
        if refresh_token:
            is_valid_token = self._get_token ()
        if is_valid_token or not refresh_token:
            # build get parameters
            params = '?' if len(data) > 0 else ''
            for param in data:
                key = param
                value = data[param]
                params = params + f'{key}={value}&'
            # query
            r = self._session.get(url + params)
            # eval response
            if (r.status_code == 200 and r.json()):
                _LOGGER.info (f"{_LABEL}: got a valid response for {url + params}")
                response = r.json()
            elif (r.status_code == 401 and not refresh_token):
                response = self._send_cmd (url, data=data, refresh_token=True)
            else:
                _LOGGER.error (f'{url + params} returned {r.text} with code {r.status_code}')
        return response

    def update (self, cups, date_from=datetime(1970, 1, 1), date_to=datetime.today()):

        def update_dictlist (old_lst, new_lst, key):
            old_copy = old_lst
            nn = []
            for n in new_lst:
                for o in old_copy:
                    if n[key] == o[key]:
                        o = n
                        break
                else:
                    nn.append (n)
            old_copy.extend(nn)
            return old_copy

        def filter_consumptions_dictlist (lst, date_from, date_to):
            new_lst = []
            wdate_from = None
            wdate_to = None
            for i in lst:
                if (date_from <= i['datetime'] <= date_to):
                    if i['value_kWh'] > 0:
                        if wdate_from is None or i['datetime'] < wdate_from:
                            wdate_from = i['datetime']
                        if wdate_to is None or i['datetime'] > wdate_to:
                            wdate_to = i['datetime']
                    new_lst.append(i)
            _LOGGER.debug (f'found data from {wdate_from} to {wdate_to}')
            return new_lst, (wdate_from, wdate_to)
            
        _LOGGER.debug (f"{_LABEL}: updating data for CUPS {cups[-4:]} from {date_from} to {date_to}")
        is_updated = False
        if (datetime.now() - self._last_try) > self.UPDATE_INTERVAL:
            is_updated = True
            self._last_try = datetime.now()
            data_bck = self.data
            self.data['consumptions'], (got_from, got_to) = filter_consumptions_dictlist (self.data['consumptions'], date_from, date_to)
            date_from = got_to if got_from is not None and got_from <= date_from else date_from
            date_to = date_to
            _LOGGER.debug (f"{_LABEL}: updating data for CUPS {cups[-4:]} from {date_from} to {date_to}")
            self.data['maximeter'] = []
            supplies = self.get_supplies ()
            self.data['supplies'] = supplies if len(supplies) > 0 else self.data['supplies']
            for supply in self.data['supplies']:
                start_in_range = supply['date_start'] <= date_from <= (supply['date_end'] if supply['date_end'] is not None else date_from)
                end_in_range = supply['date_start'] <= date_to <= (supply['date_end'] if supply['date_end'] is not None else date_to)
                if cups == supply['cups'] and (start_in_range or end_in_range):
                    _LOGGER.info (f'{_LABEL}: processing supply valid from {supply["date_start"]} to {supply["date_end"]}')
                    p_date_start = date_from if start_in_range and date_from >= supply['date_start'] else supply['date_start']
                    p_date_end = date_to if supply['date_end'] is None or (end_in_range and date_to <= supply['date_end']) else supply['date_end']
                    contracts = self.get_contract_detail (cups, supply['distributorCode'])
                    self.data['contracts'] = contracts if len(contracts) > 0 else self.data['contracts']
                    for contract in self.data['contracts']:
                        _LOGGER.info (f'{_LABEL}: processing contract valid from {contract["date_start"]} to {contract["date_end"]}')
                        start_in_range = contract['date_start'] <= date_from <= (contract['date_end'] if contract['date_end'] is not None else date_from)
                        end_in_range = contract['date_start'] <= date_to <= (contract['date_end'] if contract['date_end'] is not None else date_to)
                        if start_in_range or end_in_range:
                            p_date_start = date_from if start_in_range and date_from >= contract['date_start'] else contract['date_start']
                            p_date_end = date_to if contract['date_end'] is None or (end_in_range and date_to <= contract['date_end']) else contract['date_end']
                            _LOGGER.debug (f"{_LABEL}: fetching consumptions from {p_date_start} to {p_date_end}")
                            r = self.get_consumption_data (cups,  supply["distributorCode"],  p_date_start,  p_date_end,  "0", str(supply['pointType']))
                            self.data['consumptions'] = update_dictlist(self.data['consumptions'], r, 'datetime')
                            _LOGGER.debug (f"{_LABEL}: fetching maximeter from {p_date_start} to {p_date_end}")
                            p_date_start = min (p_date_start + relativedelta(months=1), p_date_end)
                            r = self.get_max_power (cups, supply["distributorCode"], p_date_start, p_date_end)
                            self.data['maximeter'] = update_dictlist(self.data['maximeter'], r, 'datetime')
        else:
            _LOGGER.debug ('ignoring update request due to update interval limit')
        return is_updated

    def get_supplies (self, authorizedNif=None):
        data = {}
        if authorizedNif is not None:
            data['authorizedNif'] = authorizedNif
        r = self._send_cmd("https://datadis.es/api-private/api/get-supplies", data=data)
        c = []
        for i in r:
            if all (k in i for k in ("cups", "validDateFrom", "validDateTo", 'pointType', 'distributorCode')):
                d = {
                    'cups': i['cups'],
                    'date_start': datetime.strptime (i['validDateFrom'], '%Y/%m/%d'),
                    'date_end': datetime.strptime (i['validDateTo'], '%Y/%m/%d') if i['validDateTo'] != '' else None,
                    'address': i['address'] if 'address' in i else None,
                    'postal_code': i['postalCode'] if 'postalCode' in i else None,
                    'province': i['province'] if 'province' in i else None,
                    'municipality': i['municipality'] if 'municipality' in i else None,
                    'distributor': i['distributor'] if 'distributor' in i else None,
                    'pointType': i['pointType'],
                    'distributorCode': i['distributorCode']
                }
                c.append (d)
            else:
                _LOGGER.warning (f'{_LABEL}: weird data structure while fetching supplies data, got {r}')
        return c

    def get_contract_detail (self, cups, distributorCode, authorizedNif=None):
        data = {
            'cups': cups, 
            'distributorCode': distributorCode
            }
        if authorizedNif is not None:
            data['authorizedNif'] = authorizedNif
        r = self._send_cmd("https://datadis.es/api-private/api/get-contract-detail", data=data)
        c = []
        for i in r:
            if all (k in i for k in ("startDate", "endDate", "marketer", "contractedPowerkW")):
                d = {
                    'date_start': datetime.strptime (i['startDate'], '%Y/%m/%d') if i['startDate'] != '' else None,
                    'date_end': datetime.strptime (i['endDate'], '%Y/%m/%d') if i['endDate'] != '' else None,
                    'marketer': i['marketer'],
                    'power_p1': i['contractedPowerkW'][0] if isinstance(i['contractedPowerkW'], list) else None,
                    'power_p2': i['contractedPowerkW'][1] if (len( i['contractedPowerkW']) > 1) else None
                }
                c.append (d)
            else:
                _LOGGER.warning (f'{_LABEL}: weird data structure while fetching contracts data, got {r}')
        return c

    def get_consumption_data (self, cups, distributorCode, startDate, endDate, measurementType, pointType, authorizedNif=None):
        data = {
            'cups': cups, 
            'distributorCode': distributorCode, 
            'startDate': datetime.strftime(startDate, '%Y/%m/%d'), 
            'endDate': datetime.strftime(endDate, '%Y/%m/%d'), 
            'measurementType': measurementType, 
            'pointType': pointType
            }
        if authorizedNif is not None:
            data['authorizedNif'] = authorizedNif
        r = self._send_cmd("https://datadis.es/api-private/api/get-consumption-data", data=data)
        c = []
        for i in r:
            if all (k in i for k in ("time", "date", "consumptionKWh", "obtainMethod")):
                hour = str(int(i['time'].split(':')[0]) - 1) 
                d = {
                    'datetime': datetime.strptime (f"{i['date']} {hour.zfill(2)}:00", '%Y/%m/%d %H:%M') ,
                    'value_kWh': i['consumptionKWh'] ,
                    'real': True if i['obtainMethod'] == 'Real' else False
                }
                c.append (d)
            else:
                _LOGGER.warning (f'{_LABEL}: weird data structure while fetching consumption data, got {r}')
        return c

    def get_max_power (self, cups, distributorCode, startDate, endDate, authorizedNif=None):
        data = {
            'cups': cups, 
            'distributorCode': distributorCode, 
            'startDate': datetime.strftime(startDate, '%Y/%m'), 
            'endDate': datetime.strftime(endDate, '%Y/%m')
            }
        if authorizedNif is not None:
            data['authorizedNif'] = authorizedNif
        r = self._send_cmd("https://datadis.es/api-private/api/get-max-power", data=data)
        c = []
        for i in r:
            if all (k in i for k in ("time", "date", "maxPower")):
                d = {
                    'datetime': datetime.strptime (f"{i['date']} {i['time']}", '%Y/%m/%d %H:%M') if 'date' in i and 'time' in i else None,
                    'value_kW': i['maxPower'] if 'maxPower' in i else None
                }
                c.append (d)
            else:
                _LOGGER.warning (f'{_LABEL}: weird data structure while fetching maximeter data, got {r}')
        return c
