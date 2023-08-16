# -*- coding: utf-8 -*-
# package
import pandas as pd
import time
import datetime
from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas.io.json import json_normalize
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import warnings
warnings.filterwarnings('ignore')
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:1234@localhost:3306/country_market", isolation_level='AUTOCOMMIT')
#%%
# input
MyServiceKey = ""
#%%
# 사용자 정의함수 : 낙찰내역 데이터 수집
def country_market_who(start, end):
    url_base = "https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServc?"
    ServiceKey = MyServiceKey
    table = pd.DataFrame()
    page = 1
    while True:
        url_base_m= url_base + '&inqryBgnDt='+ start + '0000' + '&inqryEndDt=' + end + '2359' + '&pageNo=' \
                    + str(page) + '&numOfRows=999' + '&ServiceKey=' + ServiceKey + '&type=json' + '&inqryDiv=1'
        r = requests.get(url_base_m,verify=False).json()
        data = r['response']['body']['items']
        df = pd.DataFrame.from_dict(json_normalize(data), orient='columns')
        if len(df.index) == 0:
            break
        table = pd.concat([table, df])
        page += 1
        time.sleep(2)
    return table
#%%
# 요청기간 추출 : 2020.01.01.~ 2023.08.14(31).
def get_first_last_day_of_month(year, month):
    first_day = datetime.date(year, month, 1)
    last_day = datetime.date(year + month // 12, month % 12 + 1, 1) - datetime.timedelta(days=1)
    return first_day.strftime('%Y%m%d'), last_day.strftime('%Y%m%d')
# input
import datetime
start_year = 2020
end_year = 2023

list_y = []
for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        first_day, last_day = get_first_last_day_of_month(year, month)
        list_y.append((first_day,last_day))

list_f = list_y[:-5]
list_f.append(('20230801', '20230831'))
#%%
data_list = []

# raw_data
start_api = pd.Timestamp.now()
for k in tqdm(range(len(list_f))):
    print(list_f[k][0],list_f[k][1])
    raw_data = country_market_who(str(list_f[k][0]), str(list_f[k][1]))
    time.sleep(1)
    data_list.append(raw_data)
    print(len(data_list))
print("Raw Data 호출시간: %s" % (pd.Timestamp.now() - start_api))

raw_data1 = pd.concat(data_list)
#%%
# 칼럼 한글화
df_col = pd.read_sql(f"SELECT * FROM 칼럼_나라장터 where 칼럼종류 = '낙찰내역'", con=engine)
temp_df = raw_data1.copy().reset_index(drop=True)
temp_df.columns = df_col['한글칼럼'].to_list()
temp_df['최종낙찰금액'] = pd.to_numeric(temp_df['최종낙찰금액'], errors='coerce').fillna(0).astype('int64')
temp_df['참가업체수'] = pd.to_numeric(temp_df['참가업체수'], errors='coerce').fillna(0).astype('int64')
temp_df['최종낙찰률'] = pd.to_numeric(temp_df['최종낙찰률'], errors='coerce').fillna(0).astype(float)
temp_df['실개찰일시'] = pd.to_datetime(temp_df['실개찰일시'], format='%Y-%m-%d %H:%M:%S', errors='raise')
temp_df['등록일시'] = pd.to_datetime(temp_df['등록일시'], format='%Y-%m-%d %H:%M:%S', errors='raise')
temp_df['최종낙찰일자'] = pd.to_datetime(temp_df['최종낙찰일자'], format='%Y-%m-%d', errors='raise')
temp_df.to_sql(name='수집_낙찰내역', con=engine, if_exists='append', index=False)
#------------------------------------------------------------------------------------------------------------
# 키워드 기반 필터링
keyword_list = ['데이터','인구']
market_list = []
for k in keyword_list:
    temp = temp_df.loc[temp_df['입찰공고명'].apply(lambda x: x.replace(" ", "")).str.contains(k) == True]
    temp['키워드'] = k
    market_list.append(temp)
df_market = pd.concat(market_list)
df_market.drop_duplicates(['입찰공고명'], keep='first', inplace=True)
df_market = df_market[df_market.columns[-1:].to_list() + df_market.columns[:-1].to_list()]
df_market = df_market.loc[df_market['입찰공고명'].apply(lambda x: x.replace(" ", "")).str.contains('분석') == True]
#%%
# DB업로드
df_market.to_sql(name='분석_낙찰내역', con=engine, if_exists='append', index=False)
#%%