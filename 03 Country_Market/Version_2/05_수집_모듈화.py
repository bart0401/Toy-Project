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
engine = create_engine("mysql+pymysql://root:1234@localhost:3306/country_market", isolation_level='AUTOCOMMIT')
#%%
class collect_info:
    def __init__(self):
        # 수집기간 설정
        if datetime.today().weekday() == 0:
            startdays = 3
        else:
            startdays = 1
        self.end = (datetime.now() - relativedelta(days=1)).strftime('%Y%m%d')
        self.start = (datetime.now() - relativedelta(days=startdays)).strftime('%Y%m%d')
        # 서비스키
        self.MyServiceKey = ""

    # API 데이터 수집
    def country_market(self, url, category, table_name):
        table = pd.DataFrame()
        page = 1
        while True:
            url_base_m = url + '&inqryBgnDt=' + self.start + '0000' + '&inqryEndDt=' + self.end + '2359' + '&pageNo=' \
                         + str(page) + '&numOfRows=999' + '&ServiceKey=' + self.MyServiceKey + '&type=json' + '&inqryDiv=1'
            r = requests.get(url_base_m, verify=False).json()
            data = r['response']['body']['items']
            df = pd.DataFrame.from_dict(json_normalize(data), orient='columns')
            if len(df.index) == 0:
                break
            table = pd.concat([table, df])
            page += 1
            time.sleep(1)
        # 칼럼 한글화
        df_col = pd.read_sql(fr"SELECT * FROM 칼럼_나라장터 where 칼럼종류 = '{category}'", con=engine)
        dict_col = dict(zip(df_col['영문칼럼'], df_col['한글칼럼']))
        table.rename(columns=dict_col, inplace=True)
        # DB 업로드
        table.to_sql(name=table_name, con=engine, if_exists='append', index=False)
#%%
if __name__ == '__main__':
    info = [
            ("https://apis.data.go.kr/1230000/BidPublicInfoService04/getBidPblancListInfoServc01?", "입찰공고", "수집_입찰공고"),
            ("http://apis.data.go.kr/1230000/HrcspSsstndrdInfoService/getPublicPrcureThngInfoServc?", "사전규격", "수집_사전규격"),
            ("https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServc?", "낙찰내역", "수집_낙찰내역")
           ]
    for url, category, table_name in info:
        collect_info().country_market(url, category, table_name)
#%%