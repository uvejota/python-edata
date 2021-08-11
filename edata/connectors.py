from abc import ABC, abstractmethod
import requests, jwt, logging, json
from bs4 import BeautifulSoup as bs
from urllib.parse import urlparse, unquote
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.WARNING)
_LOGGER = logging.getLogger(__name__)

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

class ConnectorError(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self) -> str:
        return f'{self.message}'

class BaseConnector (ABC):

    @abstractmethod
    def __init__(self, username: str, password: str):
        pass

    @abstractmethod
    def update (self, cups, date_from, date_to):
        pass

class DatadisConnector (BaseConnector):
    UPDATE_INTERVAL = timedelta(minutes=60)
    __token = {}
    __usr = None
    __pwd = None
    __session = None

    __lastAttempt = datetime(1970, 1, 1)

    def __init__(self, username, password):
        self.__usr = username
        self.__pwd = password
        self.__session = requests.Session()
        self.data = {
            'supplies': [],
            'contracts': [],
            'consumptions': [],
            'maximeter': []
        }

    def __get_token (self):
        _LOGGER.info ('DatadisConnector: no token found, fetching a new one...')
        self.__session = requests.Session()
        credentials = {'username': self.__usr, 'password': self.__pwd}
        r = self.__session.post("https://datadis.es/nikola-auth/tokens/login", data=credentials)
        if (r.status_code == 200):
            # store token both encoded and decoded
            self.__token['encoded'] = r.text
            self.__token['decoded'] = jwt.decode(self.__token['encoded'], options={"verify_signature": False})
            # prepare session authorization bearer
            self.__session.headers['Authorization'] = 'Bearer ' + self.__token['encoded']
            _LOGGER.info (f"Token received {self.__token['decoded']}")
        elif (r.status_code == 401):
            raise ConnectorError ('DatadisConnector: invalid credentials')
        else:
            raise ConnectorError ('DatadisConnector: unknown error while retrieving token')

    def __send_cmd (self, url, data={}, refresh_token=False):
        response = None
        # refresh token if needed (recursive approach)
        if refresh_token:
            self.__get_token ()
        # build get parameters
        params = '?' if len(data) > 0 else ''
        for param in data:
            key = param
            value = data[param]
            params = params + f'{key}={value}&'
        # query
        r = self.__session.get(url + params)
        # eval response
        if (r.status_code == 200 and r.json()):
            _LOGGER.info (f"DatadisConnector: got a valid response for {url + params}: {r.json()}")
            response = r.json()
        elif (r.status_code == 401 and not refresh_token):
            response = self.__send_cmd (url, data=data, refresh_token=True)
        else:
            raise ConnectorError (f'{url + params} returned {r.text} with code {r.status_code}')
        return response

    def update (self, cups, date_from=datetime(1970, 1, 1), date_to=datetime.today()):
        _LOGGER.debug (f"DatadisConnector: updating data for CUPS {cups[-4:]} from {date_from} to {date_to}")
        if (datetime.now() - self.__lastAttempt) > self.UPDATE_INTERVAL:
            self.__lastAttempt = datetime.now()
            data_bck = self.data
            self.data['consumptions'] = []
            self.data['maximeter'] = []
            try:
                self.data['supplies'] = self.get_supplies ()
                for supply in self.data['supplies']:
                    if cups == supply['cups'] and (
                        (supply['date_start'] <= date_from <= (supply['date_end'] if supply['date_end'] is not None else date_from)) or (
                            supply['date_start'] <= date_to <= (supply['date_end'] if supply['date_end'] is not None else date_to))):
                        p_date_start = max (date_from, supply['date_start'])
                        p_date_end = min (date_to, supply['date_end']) if supply['date_end'] is not None else date_to
                        self.data['contracts'] = self.get_contract_detail (cups, supply['distributorCode'])
                        for contract in self.data['contracts']:
                            if (contract['date_start'] <= date_from <= (contract['date_end'] if contract['date_end'] is not None else date_from)) or (
                            contract['date_start'] <= date_to <= (contract['date_end'] if contract['date_end'] is not None else date_to)):
                                p_date_start = max (p_date_start, contract['date_start'])
                                p_date_end = min (p_date_end, contract['date_end']) if contract['date_end'] is not None else p_date_end
                                r = self.get_consumption_data (cups,  supply["distributorCode"],  p_date_start,  p_date_end,  "0", str(supply['pointType']))
                                self.data['consumptions'] = update_dictlist(self.data['consumptions'], r, 'datetime')
                                r = self.get_max_power (cups, supply["distributorCode"], contract['date_start'] + relativedelta(months=1), p_date_end)
                                self.data['maximeter'] = update_dictlist(self.data['maximeter'], r, 'datetime')
            except Exception as e:
                if len(self.data['supplies']) == 0:
                    self.data['supplies'] = data_bck['supplies']
                elif len(self.data['contracts']) == 0:
                    self.data['contracts'] = data_bck['contracts']
                elif len(self.data['consumptions']) == 0:
                    self.data['consumptions'] = data_bck['consumptions']
                elif len(self.data['maximeter']) == 0:
                    self.data['maximeter'] = data_bck['maximeter']
                raise e
        else:
            _LOGGER.debug ('ignoring update request due to update interval limit')

    def get_supplies (self, authorizedNif=None):
        data = {}
        if authorizedNif is not None:
            data['authorizedNif'] = authorizedNif
        r = self.__send_cmd("https://datadis.es/api-private/api/get-supplies", data=data)
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
                _LOGGER.warning (f'weird data structure while fetching supplies data, got {r}')
        return c

    def get_contract_detail (self, cups, distributorCode, authorizedNif=None):
        data = {
            'cups': cups, 
            'distributorCode': distributorCode
            }
        if authorizedNif is not None:
            data['authorizedNif'] = authorizedNif
        r = self.__send_cmd("https://datadis.es/api-private/api/get-contract-detail", data=data)
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
                _LOGGER.warning (f'weird data structure while fetching contracts data, got {r}')
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
        r = self.__send_cmd("https://datadis.es/api-private/api/get-consumption-data", data=data)
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
                _LOGGER.warning (f'weird data structure while fetching consumption data, got {r}')
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
        r = self.__send_cmd("https://datadis.es/api-private/api/get-max-power", data=data)
        c = []
        for i in r:
            if all (k in i for k in ("time", "date", "maxPower")):
                d = {
                    'datetime': datetime.strptime (f"{i['date']} {i['time']}", '%Y/%m/%d %H:%M') if 'date' in i and 'time' in i else None,
                    'value_kW': i['maxPower'] if 'maxPower' in i else None
                }
                c.append (d)
            else:
                _LOGGER.warning (f'weird data structure while fetching maximeter data, got {r}')
        return c

class EdistribucionConnector(BaseConnector):
    ''' credits to @trocotronic '''
    LONG_UPDATE_INTERVAL = timedelta(minutes=60)
    SHORT_UPDATE_INTERVAL = timedelta(minutes=10)
    __session = None
    __token = 'undefined'
    __credentials = {}
    __dashboard = 'https://zonaprivada.edistribucion.com/areaprivada/s/sfsites/aura?'
    __command_index = 0
    __identities = {}
    __context = None
    __access_date = datetime.now()
    __lastLongAttempt = datetime(1970, 1, 1)
    __lastShortAttempt = datetime(1970, 1, 1)
    __retryNumber = 0
    __retryLimit = 5
    
    def __init__(self, user, password):
        self.__credentials['user'] = user
        self.__credentials['password'] = password
        self.__session = requests.Session()
        self.data = {
            'supplies': [],
            'contracts': [],
            'consumptions': [],
            'maximeter': []
        }
        
    def __get_url(self, url,get=None,post=None,json=None,cookies=None,headers=None):
        _headers = {
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:77.0) Gecko/20100101 Firefox/77.0',
        }
        if (headers):
            _headers.update(headers)
        if (post is None and json is None):
            r = self.__session.get(url, params=get, headers=_headers, cookies=cookies)
        else:
            r = self.__session.post(url, data=post, json=json, params=get, headers=_headers, cookies=cookies)
        if r.status_code >= 400:
            raise ConnectorError (f'EdistribucionConnector: {url} returned {r.text} with code {r.status_code}')
        return r
    
    def __send_cmd(self, command, post=None, dashboard=None, accept='*/*', content_type=None):

        if dashboard is None: dashboard = self.__dashboard 

        if (self.__command_index):
            command = 'r='+self.__command_index+'&'
            self.__command_index += 1
        
        if (post):
            post['aura.context'] = self.__context
            post['aura.pageURI'] = '/areaprivada/s/wp-online-access'
            post['aura.token'] = self.__token

        headers = {}
        headers['Accept'] = accept
        if content_type is not None:
            headers['Content-Type'] = content_type

        r = self.__get_url(dashboard+command, post=post, headers=headers)
        if ('window.location.href' in r.text or 'clientOutOfSync' in r.text):
                _LOGGER.info ('Redirection received. Aborting command.')
        elif ('json' in r.headers['Content-Type']):
            jr = r.json()
            if (jr['actions'][0]['state'] != 'SUCCESS'):
                _LOGGER.info ('Got an error. Aborting command.')
                raise ConnectorError (f'EdistribucionConnector: error while processing command: {command}')
            return jr['actions'][0]['returnValue']
        
        return r
        
    def __get_token(self):
        if (not (self.__token != 'undefined' and self.__access_date+timedelta(minutes=10) > datetime.now())):
            _LOGGER.debug('Login')
            self.__session = requests.Session()
            r = self.__get_url('https://zonaprivada.edistribucion.com/areaprivada/s/login?ec=302&startURL=%2Fareaprivada%2Fs%2F')
            ix = r.text.find('auraConfig')
            if (ix == -1):
                raise ConnectorError ('EdistribucionConnector: auraConfig not found. Cannot continue')
            soup = bs(r.text, 'html.parser')
            scripts = soup.find_all('script')
            for s in scripts:
                src = s.get('src')
                if (not src):
                    continue
                #print(s)
                upr = urlparse(r.url)
                r = self.__get_url(upr.scheme+'://'+upr.netloc+src)
                if ('resources.js' in src):
                    unq = unquote(src)
                    self.__context = unq[unq.find('{'):unq.rindex('}')+1]
                    self.__appInfo = json.loads(self.__context)
            _LOGGER.debug('EdistribucionConnector: performing login routine')
            data = {
                    'message':'{"actions":[{"id":"91;a","descriptor":"apex://LightningLoginFormController/ACTION$login","callingDescriptor":"markup://c:WP_LoginForm","params":{"username":"'+self.__credentials['user']+'","password":"'+self.__credentials['password']+'","startUrl":"/areaprivada/s/"}}]}',
                    'aura.context':self.__context,
                    'aura.pageURI':'/areaprivada/s/login/?language=es&startURL=%2Fareaprivada%2Fs%2F&ec=302',
                    'aura.token':'undefined',
                    }
            r = self.__get_url(self.__dashboard+'other.LightningLoginForm.login=1',post=data)
            #print(r.text)
            if ('/*ERROR*/' in r.text):
                if ('invalidSession' in r.text):
                    self.__session = requests.Session()
                    self.__get_token()
                raise ConnectorError ('EdistribucionConnector: login failed, credentials might be wrong')
            jr = r.json()
            if ('events' not in jr):
                raise ConnectorError ('EdistribucionConnector: login failed, credentials might be wrong')
            
            _LOGGER.debug('Accessing to frontdoor')
            r = self.__get_url(jr['events'][0]['attributes']['values']['url'])
            _LOGGER.debug('Accessing to landing page')
            r = self.__get_url('https://zonaprivada.edistribucion.com/areaprivada/s/')
            ix = r.text.find('auraConfig')
            if (ix == -1):
                raise ConnectorError ('EdistribucionConnector: auraConfig not found. Cannot continue')
            ix = r.text.find('{',ix)
            ed = r.text.find(';',ix)
            try:
                jr = json.loads(r.text[ix:ed])
            except Exception:
                jr = {}
            if ('token' not in jr):
                raise ConnectorError ('EdistribucionConnector: token not found. Cannot continue')
            self.__token = jr['token']
            _LOGGER.debug('EdistribucionConnector: token received!')
            r = self.__getLoginInfo()
            self.__identities['account_id'] = r['visibility']['Id']
            self.__identities['name'] = r['Name']

    def update (self, cups, date_from=datetime(1970, 1, 1), date_to=datetime.today()):
        if (datetime.now() - self.__lastShortAttempt) > (self.__retryNumber + 1)*self.SHORT_UPDATE_INTERVAL:
            self.__lastShortAttempt = datetime.now()
            data_bck = self.data
            self.data['consumptions'] = []
            self.data['maximeter'] = []
            try:
                self.data['supplies'] = self.get_supplies ()
                for supply in self.data['supplies']:
                    if cups == supply['cups'] and (
                        (supply['date_start'] <= date_from <= (supply['date_end'] if supply['date_end'] is not None else date_from)) or (
                            supply['date_start'] <= date_to <= (supply['date_end'] if supply['date_end'] is not None else date_to))):
                        p_date_start = max (date_from, supply['date_start'])
                        p_date_end = min (date_to, supply['date_end']) if supply['date_end'] is not None else date_to
                        self.data['contracts'] = self.get_contract_detail (cups)
                        if (datetime.now() - self.__lastLongAttempt) > self.LONG_UPDATE_INTERVAL:
                            self.__lastLongAttempt = datetime.now()
                            for contract in self.data['contracts']:
                                if (contract['date_start'] <= date_from <= (contract['date_end'] if contract['date_end'] is not None else date_from)) or (
                                    contract['date_start'] <= date_to <= (contract['date_end'] if contract['date_end'] is not None else date_to)):
                                    p_date_start = max (p_date_start, contract['date_start'])
                                    p_date_end = min (p_date_end, contract['date_end']) if contract['date_end'] is not None else p_date_end
                                    p_start = p_date_start
                                    finish = False
                                    r = []
                                    while not finish:
                                        p_end = min(p_start + timedelta (days=50), p_date_end)
                                        r.extend(self.get_consumption_data (contract['cont_id'],  p_start,  p_end))
                                        finish = (p_date_end == p_end)
                                        p_start = p_end + timedelta(days=1)
                                    self.data['consumptions'] = update_dictlist(self.data['consumptions'], r, 'datetime')
                                    r = self.get_max_power (contract['cups_id'], contract['date_start'] + relativedelta(months=1), p_date_end)
                                    self.data['maximeter'] = update_dictlist(self.data['maximeter'], r, 'datetime')
                        #if supply['date_end'] is None:
                        #    self.data['meter'] = self.get_meter_data (supply['cups_id'])
                self.__retryNumber = 0
            except Exception as e:
                self.data = data_bck
                self.__retryNumber = min(self.__retryLimit, self.__retryNumber + 1)
                raise e
        else:
            _LOGGER.debug ('EdistribucionConnector: ignoring update request due to update interval limit')

    def get_supplies (self):
        supplies = []
        self.__get_token()
        lst = self.__getListCups ()
        for s in lst['data']['lstCups']:
            new_supply = {
                'cups': s['CUPs__r']['Name'],
                'date_start': datetime.strptime(s['Version_start_date__c'],'%Y-%m-%d'),
                'date_end': datetime.strptime(s['Version_end_date__c'],'%Y-%m-%d') if 'Version_end_date__c' in s else None,
                'address': s['Provisioning_address__c'] if 'Provisioning_address__c' in s else None,
                'postal_code': s['postal-code'] if 'postal-code' in s else None,
                'province': s['city'],
                'municipality': s['CUPs__r']['NS_Town_Description__c'],
                'distributor': 'EDISTRIBUCIÓN',
                'cups_id': s['CUPs__r']['Id']
            }
            supplies.append(new_supply)
        return supplies

    def get_contract_detail (self, cups):
        contracts = []
        self.__get_token()
        lst = self.__getListCups ()
        for c in lst['data']['lstContAux']:
            if c['CUPs__r']['Name'] == cups:
                d = self.__getATRDetail (c['Id'])['data']
                power_limit_p1 = None
                power_limit_p2 = None
                marketer = None
                for item in d:
                    if 'title' in item:
                        if item['title'] == 'Potencia contratada 1 (kW)':
                            power_limit_p1 = float(item['value'].replace(",", ".")) if item['value'] != '' else None
                        elif item['title'] == 'Potencia contratada 2 (kW)':
                            power_limit_p2 = float(item['value'].replace(",", ".")) if item['value'] != '' else None
                        elif item['title'] == 'Comercializadora':
                            marketer = item['value']
                new_contract = {
                    'date_start': datetime.strptime(c['Version_start_date__c'],'%Y-%m-%d'),
                    'date_end': datetime.strptime(c['Version_end_date__c'],'%Y-%m-%d') if 'Version_end_date__c' in c else None,
                    'marketer': marketer,
                    'power_p1': power_limit_p1,
                    'power_p2': power_limit_p2,
                    'cups_id': c['CUPs__c'],
                    'cont_id': c['Id']
                }
                contracts.append(new_contract)
        return contracts
            
    def get_consumption_data (self, cont_id, startDate, endDate):
        consumptions = []
        self.__get_token()
        start_str = startDate.strftime ("%Y-%m-%d")
        end_str = endDate.strftime ("%Y-%m-%d")
        c = self.__getChartPointsByRange (cont_id, start_str, end_str)['data']['lstData']
        for d in c:
            for i in d:
                if i['hourCCH'] <= 24:
                    hour = i['hourCCH'] - 1
                    i = {
                        'datetime': datetime.strptime (f"{i['date']} {hour:02d}:00", '%d/%m/%Y %H:%M'),
                        'value_kWh': i['valueDouble'] if 'valueDouble' in i else 0, 
                        'real': i['real']
                    }
                    consumptions.append(i)
        return consumptions

    def get_max_power (self, cups_id, startDate, endDate):
        maximeter = []
        self.__get_token()
        m = self.__getHistogramPoints (cups_id, startDate.strftime("%m/%Y"), endDate.strftime("%m/%Y"))['data']['lstData']
        [maximeter.append({
                    'datetime': datetime.strptime (f"{i['date']} {i['hour']}", '%d-%m-%Y %H:%M'),
                    'value_kW': i['value']
                }) for i in m if i['valid']]
        return maximeter

    def get_meter_data (self, cups_id):
        meter = {}
        self.__get_token()
        r = self.__consultarContador (cups_id)['data']
        meter['power_kw'] = r['potenciaActual'] if 'potenciaActual' in r else None
        meter['power_%'] = (r['potenciaActual'] / r['potenciaContratada']) if ('potenciaActual' in r and 'potenciaContratada' in r) else None
        meter['energy_kwh'] = int(r['totalizador'].replace(".","")) if 'totalizador' in r else None
        meter['icp_status'] = r['estadoICP'] if 'estadoICP' in r else None
        return meter

    '''
    TBD
    '''
    def get_cycles_data (self, cups):
        pass

    def __getLoginInfo(self):
        cmd = 'other.WP_Monitor_CTRL.getLoginInfo=1'
        msg = '{"actions":[{"id":"215;a","descriptor":"apex://WP_Monitor_CTRL/ACTION$getLoginInfo","callingDescriptor":"markup://c:WP_Monitor","params":{"serviceNumber":"S011"}}]}'
        return self.__send_cmd (cmd, post={'message': msg})
        
    def __getListCups(self):
        msg = '{"actions":[{"id":"1086;a","descriptor":"apex://WP_Measure_v3_CTRL/ACTION$getListCups","callingDescriptor":"markup://c:WP_Measure_List_v4","params":{"sIdentificador":"'+self.__identities['account_id']+'"}}]}',
        cmd = 'other.WP_Measure_v3_CTRL.getListCups=1'
        return self.__send_cmd (cmd, post={'message': msg})

    def __getATRDetail(self, atr):
        msg = '{"actions":[{"id":"62;a","descriptor":"apex://WP_ContractATRDetail_CTRL/ACTION$getATRDetail","callingDescriptor":"markup://c:WP_SuppliesATRDetailForm","params":{"atrId":"'+atr+'"}}]}',
        cmd = 'other.WP_ContractATRDetail_CTRL.getATRDetail=1'
        return self.__send_cmd (cmd, post={'message': msg})

    def __getChartPointsByRange (self, cont, date_start, date_end):
        msg = '{"actions":[{"id":"981;a","descriptor":"apex://WP_Measure_v3_CTRL/ACTION$getChartPointsByRange","callingDescriptor":"markup://c:WP_Measure_Detail_Filter_Advanced_v3","params":{"contId":"'+cont+'","type":"4","startDate":"'+date_start+'","endDate":"'+date_end+'"},"version":null,"longRunning":true}]}'
        cmd = 'other.WP_Measure_v3_CTRL.getChartPointsByRange=1'
        return self.__send_cmd (cmd, post={'message': msg})
        
    def __getHistogramPoints (self, cups_id, date_start, date_end):
        msg = '{"actions":[{"id":"688;a","descriptor":"apex://WP_MaximeterHistogram_CTRL/ACTION$getHistogramPoints","callingDescriptor":"markup://c:WP_MaximeterHistogramDetail","params":{"mapParams":{"startDate":"'+date_start+'","endDate":"'+date_end+'","id":"'+cups_id+'","sIdentificador":"'+self.__identities['account_id']+'"}}}]}',
        cmd = 'other.WP_MaximeterHistogram_CTRL.getHistogramPoints=1'
        return self.__send_cmd (cmd, post={'message': msg})
    
    def __consultarContador(self, cups_id):
        msg = '{"actions":[{"id":"471;a","descriptor":"apex://WP_ContadorICP_F2_CTRL/ACTION$consultarContador","callingDescriptor":"markup://c:WP_Reconnect_Detail_F2","params":{"cupsId":"'+cups_id+'"}}]}',
        cmd = 'other.WP_ContadorICP_F2_CTRL.consultarContador=1'
        return self.__send_cmd (cmd, post={'message': msg})

    def __get_cycle_list(self, cont):
        msg = '{"actions":[{"id":"1190;a","descriptor":"apex://WP_Measure_v3_CTRL/ACTION$getInfo","callingDescriptor":"markup://c:WP_Measure_Detail_v4","params":{"contId":"'+cont+'"},"longRunning":true}]}',
        cmd = 'other.WP_Measure_v3_CTRL.getInfo=1'
        return self.__send_cmd (cmd, post={'message': msg})
