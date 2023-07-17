# -*- coding: utf-8 -*-

"""
[ 오퍼레이션(영문) ]
사전규격 물품 목록 조회 : getPublicPrcureThngInfoThng
사전규격 외자 목록 조회 : getPublicPrcureThngInfoFrgcpt
사전규격 용역 목록 조회 : getPublicPrcureThngInfoServc
사전규격 공사 목록 조회 : getPublicPrcureThngInfoCnstwk
"""

# package
import pandas as pd
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas.io.json import json_normalize
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from sqlalchemy import create_engine
engine = create_engine("postgresql://postgres:1234@localhost:5432/country_market", isolation_level='AUTOCOMMIT')
#%%
# input
MyServiceKey = ""
#%%
# 사용자 정의함수 : 사전규격 데이터 수집
def country_market_pre(start, end):
    url_base = fr"http://apis.data.go.kr/1230000/HrcspSsstndrdInfoService/getPublicPrcureThngInfoServc?"
    ServiceKey = MyServiceKey
    table = pd.DataFrame()
    page = 1
    while True:
        url_base_m = url_base + '&inqryBgnDt=' + start + '0000' + '&inqryEndDt=' + end + '2359' + '&pageNo=' \
                     + str(page) + '&numOfRows=999' + '&ServiceKey=' + ServiceKey + '&type=json' + '&inqryDiv=1'
        r = requests.get(url_base_m, verify=False).json()
        data = r['response']['body']['items']
        df = pd.DataFrame.from_dict(json_normalize(data), orient='columns')
        if len(df.index) == 0:
            break
        table = table.append(df)
        page += 1
        time.sleep(1)
    return table
#%%
# 수집기간 설정
if datetime.today().weekday() == 0:
    startdays = 3
else:
    startdays = 1

now = datetime.now()  # 오늘
end = now - relativedelta(days=1)  # 1일 전
end = end.strftime('%Y%m%d')

start = now - relativedelta(days=startdays)
start = start.strftime('%Y%m%d')
#%%
# 데이터 수집
raw_data = country_market_pre(start, end)
#------------------------------------------------------------------------------------------------------------
# 칼럼 한글화
df_col = pd.read_sql(f"SELECT * FROM 칼럼_나라장터 where 칼럼종류 = '사전규격'", con=engine)
temp_df = raw_data.copy().reset_index(drop=True)
temp_df.columns = df_col['한글칼럼'].to_list()
#------------------------------------------------------------------------------------------------------------
# 키워드 기반 필터링
keyword_list = ['성장관리계획', '성장관리방안', '데이터기반', '딥러닝', '머신러닝', '기계학습', '인공지능',
                '인구소멸', '지방소멸', '인구활력', '인구감소', '인구정책', '인구시책', '활성화계획', '인구', '탄소중립', '인구활력',
                '상권정보', '상권분석', '점포개발', '입지분석', '입지개발', 'CRM', '유통분석', 'GIS', '공간분석',
                '전세사기', '소유자정보', '주택시세', '부동산데이터', '부동산시세', '부동산AI',
                '빅데이터', '알고리즘', 'AI분석', 'AI', '부동산빅데이터', '부동산입지',
                '기업은행', '농협은행', '소상공인', '국방부', '부동산원', '농림축산', '농림', '축산', '플랫폼']
market_list = []
for k in keyword_list:
    temp = temp_df.loc[temp_df['품명'].apply(lambda x: x.replace(" ", "")).str.contains(k) == True]
    if startdays == 1:
        temp['수집기간'] = fr'{start}'
    else:
        temp['수집기간'] = fr'{start}~{end}'
    temp['키워드'] = k
    market_list.append(temp)
df_market = pd.concat(market_list)
df_market.drop_duplicates(['품명'], keep='first', inplace=True)
df_market = df_market[df_market.columns[-2:].to_list() + df_market.columns[:-2].to_list()]

# 가격칼럼 수정
df_market['배정예산금액'] = pd.to_numeric(df_market['배정예산금액'], errors='coerce').fillna(0).astype('int64')
#%%
# DB업로드
df_market.to_sql(name='수집_사전규격', con=engine, if_exists='append', index=False)
#%%
