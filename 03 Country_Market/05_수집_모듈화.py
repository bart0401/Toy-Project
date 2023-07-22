# -*- coding: utf-8 -*-

# package
import pandas as pd
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas import json_normalize
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from sqlalchemy import create_engine
engine = create_engine("postgresql://postgres:1234@localhost:5432/country_market", isolation_level='AUTOCOMMIT')
#%%
class collect_info:
    def __init__(self):
        # 서비스키
        self.MyServiceKey = ""
        # URL
        self.url1 = "https://apis.data.go.kr/1230000/BidPublicInfoService04/getBidPblancListInfoServc01?"  # 입찰공고정보
        self.url2 = "http://apis.data.go.kr/1230000/HrcspSsstndrdInfoService/getPublicPrcureThngInfoServc?"  # 사전규격
        self.url3 = "https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServc?"  # 낙찰내역
        # 수집기간
        if datetime.today().weekday() == 0:
            startdays = 3
        else:
            startdays = 1
        self.end = (datetime.now() - relativedelta(days=1)).strftime('%Y%m%d')
        self.start = (datetime.now() - relativedelta(days=startdays)).strftime('%Y%m%d')

    def country_market(self, url_base):
        self.url_base = url_base
        table = pd.DataFrame()
        page = 1
        while True:
            url_base_m = url_base + '&inqryBgnDt=' + self.start + '0000' + '&inqryEndDt=' + self.end + '2359' + '&pageNo=' \
                         + str(page) + '&numOfRows=999' + '&ServiceKey=' + self.MyServiceKey + '&type=json' + '&inqryDiv=1'
            r = requests.get(url_base_m, verify=False).json()
            data = r['response']['body']['items']
            df = pd.DataFrame.from_dict(json_normalize(data), orient='columns')
            if len(df.index) == 0:
                break
            table = pd.concat([table, df])
            page += 1
            time.sleep(1)
        return table
#%%
# URL
url1 = "https://apis.data.go.kr/1230000/BidPublicInfoService04/getBidPblancListInfoServc01?"  # 입찰공고정보
url2 = "http://apis.data.go.kr/1230000/HrcspSsstndrdInfoService/getPublicPrcureThngInfoServc?"  # 사전규격
url3 = "https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServc?"  # 낙찰내역

raw_data1 = collect_info().country_market(url1)
raw_data2 = collect_info().country_market(url2)
raw_data3 = collect_info().country_market(url3)