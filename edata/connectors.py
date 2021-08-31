import requests, jwt, logging
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from edata.processors import DataUtils as du
# EsiosConnector:
from aiopvpc import PVPCData, TARIFFS
import pytz as tz

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)

_LABEL = 'Connector'

class DatadisConnector ():
    UPDATE_INTERVAL = timedelta(minutes=60)
    _token = {}
    _usr = None
    _pwd = None
    _session = None

    _last_try = datetime(1970, 1, 1)    

    def __init__(self, username, password, data = {'supplies': [], 'contracts': [], 'consumptions': [], 'maximeter': []}, log_level=logging.WARNING):
        logging.getLogger().setLevel(log_level)
        self._usr = username
        self._pwd = password
        self._session = requests.Session()
        self.data = data
        self.status = {
            'supplies': False,
            'contracts': False,
            'consumptions': False,
            'maximeter': False
        }
        self.last_update = {
            'supplies': datetime(1970, 1, 1),
            'contracts': datetime(1970, 1, 1),
            'consumptions': datetime(1970, 1, 1),
            'maximeter': datetime(1970, 1, 1)
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
            _LOGGER.info (f"{_LABEL}: token received")
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
                _LOGGER.debug (f"{_LABEL}: got a valid response for {url + params}")
                response = r.json()
            elif (r.status_code == 401 and not refresh_token):
                response = self._send_cmd (url, data=data, refresh_token=True)
            elif (r.status_code == 200):
                _LOGGER.error (f'{url + params} returned a valid but empty response, try again later')
            else:
                _LOGGER.error (f'{url + params} returned {r.text} with code {r.status_code}')
        return response

    def update (self, cups, date_from=datetime(1970, 1, 1), date_to=datetime.today()):            
        _LOGGER.info (f"{_LABEL}: update requested for CUPS {cups[-4:]} from {date_from} to {date_to}")
        is_updated = False
        if (datetime.now() - self._last_try) > self.UPDATE_INTERVAL:
            is_updated = True
            self._last_try = datetime.now()
            self.data['consumptions'], missing = du.extract_dt_ranges (self.data['consumptions'], date_from, date_to, gap_interval=timedelta(hours=6))
            _LOGGER.info (f"{_LABEL}: missing data: {missing}")
            for gap in missing:
                date_from = gap['from']
                date_to = gap['to']
                _LOGGER.info (f"{_LABEL}: requesting missing data for {cups[-4:]} from {date_from} to {date_to}")
                # request supplies
                self._update_supplies ()
                supply_found = False
                for supply in self.data['supplies']:
                    supply_found = True
                    start_in_range = supply['date_start'] <= date_from <= supply['date_end']
                    end_in_range = supply['date_start'] <= date_to <= supply['date_end']
                    if cups == supply['cups']:
                        if (start_in_range or end_in_range):
                            _LOGGER.info (f"{_LABEL}: found a valid supply for CUPS {cups[-4:]} with range from {supply['date_start']} to {supply['date_end']}")
                            # request contracts
                            self._update_contracts (cups, supply['distributorCode'])
                            for contract in self.data['contracts']:
                                start_in_range = contract['date_start'] <= date_from <= contract['date_end']
                                end_in_range = contract['date_start'] <= date_to <= contract['date_end']
                                if (contract['distributorCode'] == supply['distributorCode']) and (start_in_range or end_in_range):
                                    # request consumptions
                                    _LOGGER.info (f"{_LABEL}: found a valid contract for CUPS {cups[-4:]} with range from {contract['date_start']} to {contract['date_end']}")
                                    p_date_start = max ([date_from, supply['date_start'], contract['date_start']])
                                    p_date_end = min ([date_to, supply['date_end'], contract['date_end']])
                                    _LOGGER.info (f"{_LABEL}: fetching consumptions from {p_date_start} to {p_date_end}")
                                    self._update_consumptions (cups,  supply["distributorCode"],  p_date_start,  p_date_end,  "0", supply['pointType'])
                                    # request maximeter
                                    m_date_start = max ([date_from, supply['date_start'], contract['date_start']]) + relativedelta(months=1)
                                    m_date_end = min ([date_to, supply['date_end'], contract['date_end']])
                                    if m_date_end > m_date_start:
                                        _LOGGER.info (f"{_LABEL}: fetching maximeter from {m_date_start} to {m_date_end}")
                                        self._update_maximeter (cups, supply["distributorCode"], m_date_start, m_date_end)
                                    else:
                                        _LOGGER.info (f"{_LABEL}: ignoring maximeter update for current contract (there is no need to update data / negative delta)")
                                    
                if not supply_found and (len (self.data['supplies']) > 0):
                    _LOGGER.warning (f"CUPS {cups[-4:]} not found in {self.data['supplies']}, it might be wrong, please check")
                elif not supply_found:
                    _LOGGER.warning (f"CUPS {cups[-4:]} not found in {self.data['supplies']} because list is empty")
        else:
            _LOGGER.debug ('ignoring update request due to update interval limit')
        _LOGGER.info (f"{_LABEL}: update report {self.status}")
        return is_updated

    def _update_supplies (self):
        supplies = self.get_supplies () if (datetime.today().date() != self.last_update['supplies'].date()) or (len (self.data['supplies']) == 0) else []
        if len (supplies) > 0:
            self.status['supplies'] = True
            self.data['supplies'] = supplies
            self.last_update['supplies'] = datetime.now()
            _LOGGER.info (f"{_LABEL}: supplies data has been successfully updated ({len(supplies)} elements)")
        else:
            self.status['supplies'] = False
            _LOGGER.debug (f"{_LABEL}: supplies data was not updated")

    def _update_contracts (self, cups, distributorCode, authorizedNif=None):
        contracts = self.get_contract_detail (cups, distributorCode) if (datetime.today().date() != self.last_update['contracts'].date()) or (len (self.data['contracts']) == 0) else []
        if len (contracts) > 0:
            self.status['contracts'] = True
            self.data['contracts'] = du.extend_by_key (self.data['contracts'], contracts, 'date_start')
            self.last_update['contracts'] = datetime.now()
            _LOGGER.info (f"{_LABEL}: contracts data has been successfully updated ({len(contracts)} elements)")
        else:
            self.status['contracts'] = False
            _LOGGER.debug (f"{_LABEL}: contracts data was not updated")

    def _update_consumptions (self, cups, distributorCode, startDate, endDate, measurementType, pointType, authorizedNif=None):
        r = self.get_consumption_data (cups, distributorCode, startDate, endDate, measurementType, pointType, authorizedNif=None)
        if len (r) > 0:
            self.status['consumptions'] = True
            self.data['consumptions'] = du.extend_by_key (self.data['consumptions'], r, 'datetime')
            self.last_update['consumptions'] = datetime.now()
            _LOGGER.info (f"{_LABEL}: consumptions data has been successfully updated ({len(r)} elements)")
        else:
            self.status['consumptions'] = False
            _LOGGER.debug (f"{_LABEL}: consumptions data was not updated")

    def _update_maximeter (self, cups, distributorCode, startDate, endDate, authorizedNif=None):
        r = self.get_max_power (cups, distributorCode, startDate, endDate)
        if len (r) > 0:
            self.status['maximeter'] = True
            self.data['maximeter'] = du.extend_by_key (self.data['maximeter'], r, 'datetime')
            self.last_update['maximeter'] = datetime.now()
            _LOGGER.info (f"{_LABEL}: maximeter data has been successfully updated ({len(r)} elements)")
        else:
            self.status['maximeter'] = False
            _LOGGER.debug (f"{_LABEL}: maximeter data was not updated")

    def get_supplies (self, authorizedNif=None):
        data = {}
        if authorizedNif is not None:
            data['authorizedNif'] = authorizedNif
        r = self._send_cmd("https://datadis.es/api-private/api/get-supplies", data=data)
        c = []
        tomorrow_str = (datetime.today() + timedelta (days=1)).strftime('%Y/%m/%d')
        for i in r:
            if all (k in i for k in ("cups", "validDateFrom", "validDateTo", 'pointType', 'distributorCode')):
                d = {
                    'cups': i['cups'],
                    'date_start': datetime.strptime (i['validDateFrom'] if i['validDateFrom'] != '' else '1970/01/01', '%Y/%m/%d'),
                    'date_end': datetime.strptime (i['validDateTo'] if i['validDateTo'] != '' else tomorrow_str, '%Y/%m/%d'),
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
        tomorrow_str = (datetime.today() + timedelta (days=1)).strftime('%Y/%m/%d')
        for i in r:
            if all (k in i for k in ("startDate", "endDate", "marketer", "contractedPowerkW")):
                d = {
                    'date_start': datetime.strptime (i['startDate'] if i['startDate'] != '' else '1970/01/01', '%Y/%m/%d'),
                    'date_end': datetime.strptime (i['endDate'] if i['endDate'] != '' else tomorrow_str, '%Y/%m/%d'),
                    'marketer': i['marketer'],
                    'distributorCode': distributorCode,
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
            if i.get('consumptionKWh', 0) > 0:
                if all (k in i for k in ("time", "date", "consumptionKWh", "obtainMethod")):
                    hour = str(int(i['time'].split(':')[0]) - 1) 
                    d = {
                        'datetime': datetime.strptime (f"{i['date']} {hour.zfill(2)}:00", '%Y/%m/%d %H:%M') ,
                        'delta_h': 1,
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

class EsiosConnector ():
    
    UPDATE_INTERVAL = timedelta(hours=24)
    _LABEL = 'EsiosConnector'
    _last_try = datetime(1970, 1, 1)
    
    def __init__ (self, local_timezone='Europe/Madrid', data={'pvpc': []}, log_level=logging.WARNING):
        logging.getLogger().setLevel(log_level)
        logging.getLogger("aiopvpc").setLevel(logging.ERROR)
        self._local_timezone = local_timezone
        self._handler = PVPCData(tariff=TARIFFS[0], local_timezone=self._local_timezone)
        self.data = data

    def update (self, date_from, date_to):
        if (datetime.now() - self._last_try) > self.UPDATE_INTERVAL:
            self.data['pvpc'], missing = du.extract_dt_ranges (self.data['pvpc'], date_from, date_to, gap_interval=timedelta(hours=1))
            for gap in missing:
                raw = self._handler.download_prices_for_range(gap['from'], gap['to'])
                pvpc = [
                    {
                        'datetime': datetime.strptime(x.astimezone(tz.timezone(self._local_timezone)).strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M'), 
                        'price': raw[x]
                    } for x in raw
                ]
                self.data['pvpc'] = du.extend_by_key (self.data['pvpc'], pvpc, 'datetime')
        else:
            _LOGGER.debug (f'{self._LABEL}: ignoring update request due to update interval limit')
