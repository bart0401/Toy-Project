# -*- coding: utf-8 -*-

# package
import pandas as pd
import json
import time
import datetime
import os
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import urllib.request
from pandas.io.json import json_normalize
import numpy as np
import configparser, json, sys
import requests
from bs4 import BeautifulSoup
import html5lib
import lxml
from urllib import parse
from tqdm import tqdm
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('mode.chained_assignment',  None)

# input
MyServiceKey = ""

def country_market(start, end):
    url_base = "https://apis.data.go.kr/1230000/BidPublicInfoService04/getBidPblancListInfoServc01?"
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
        table = table.append(df)
        page += 1
        time.sleep(2)
    return table

def get_first_last_day_of_month(year, month):
    first_day = datetime.date(year, month, 1)
    last_day = datetime.date(year + month // 12, month % 12 + 1, 1) - datetime.timedelta(days=1)
    return first_day.strftime('%Y%m%d'), last_day.strftime('%Y%m%d')
#%%
# input
import datetime

start_year = 2018
end_year = 2023

list_y = []

for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        first_day, last_day = get_first_last_day_of_month(year, month)
        list_y.append((first_day,last_day))

list_f = list_y[:-8]
#%%
data_list = []

# raw_data
start_api = pd.Timestamp.now()
for k in tqdm(range(len(list_f))):
    print(list_f[k][0],list_f[k][1])
    raw_data = country_market(str(list_f[k][0]), str(list_f[k][1]))
    data_list.append(raw_data)
    print(len(data_list))
print("Raw Data 호출시간: %s" % (pd.Timestamp.now() - start_api))

raw_data = pd.concat(data_list)
path9 = ''
raw_data.to_csv(fr'{path9}/raw_data_입찰공고_20180101~20230430.txt',sep='|', index=False)
#%%
df_market = raw_data.copy().reset_index(drop=True)
df_market.drop(columns=['rgnLmtBidLocplcJdgmBssCd', 'rgnLmtBidLocplcJdgmBssNm'],inplace=True)
path = ''
df_col = pd.read_excel(fr'{path}/입찰공고조회 칼럼.xlsx',header=None)
df_market.columns = df_col[1].to_list()

check = df_market.loc[df_market['입찰공고명'].str.contains('')]

df_market = df_market.loc[df_market['공고기관코드']=="B553077"]  # 공고기관코드 : B553077, 공고기관명 : 소상공인시장진흥공단
df_market = df_market.loc[~df_market['공고종류명'].str.contains('취소')==True]
df_market.sort_values(by=["입찰공고일시"], ascending=[False], inplace=True)
df_market.drop_duplicates(['입찰공고번호'], keep='first', inplace=True)
df_market.drop_duplicates(['입찰공고명'], keep='first', inplace=True)

use_cols = ['입찰공고번호','입찰공고일시','참조번호','입찰공고명','공고기관명','수요기관명','입찰방식명','계약체결방법명','개찰일시',
           '배정예산금액','추정가격','용역구분명','공동수급구성방식명','낙찰방법명','입찰공고상세URL']
df_market2 = df_market[use_cols]
#%%
market_list = df_market2['입찰공고번호'].tolist()
# bid_datail
bid_datail = pd.DataFrame()
for i in tqdm(market_list):
    try:
        url_test = 'https://www.g2b.go.kr:8101/ep/result/serviceBidResultDtl.do?&bidno=' + str(i) + '&bidseq=00'
        table_df_list = pd.read_html(url_test)
        table_df = table_df_list[1]
        table_df['입찰공고번호'] = i
        table_df['개찰완료 조회가능 여부'] = '가능'
        bid_datail = bid_datail.append(table_df.iloc[0])
    except:
        continue

bid_datail.columns = list(map(lambda x: x+str(1),bid_datail.columns.tolist()))
bid_datail.rename(columns={'입찰공고번호1':'입찰공고번호'}, inplace = True)
bid_datail.info()
#%%
df_market2.fillna('', inplace=True)
df_market4 = pd.merge(df_market2, bid_datail[['입찰공고번호','업체명1','입찰금액(원)1']], on='입찰공고번호', how='left')

df_market4['배정예산금액'] = pd.to_numeric(df_market4['배정예산금액'], errors='coerce').fillna(0).astype('int64')
df_market4['입찰금액(원)1'] = pd.to_numeric(df_market4['입찰금액(원)1'], errors='coerce').fillna(0).astype('int64')
df_market4.rename(columns={'업체명1': '업체명', '입찰금액(원)1': '입찰금액(원)'}, inplace=True)
#%%