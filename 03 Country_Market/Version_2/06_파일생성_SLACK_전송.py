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
keyword_list = ['빅데이터','데이터','딥러닝','머신러닝','기계학습','인공지능', '알고리즘', 'AI', '데이터분석', '데이터 분석',
                '인구활력','인구소멸','지방소멸','인구활력','인구감소','인구정책','인구시책','활성화계획','인구',
                '탄소중립', '상권', '점포개발', '입지', 'CRM', '유통분석', 'GIS','공간분석',
                '전세사기', '소유자정보', '주택시세', '부동산', '시세', '부동산입지',
                '은행', '소상공인', '국방부', '부동산원', '농림','축산', '감염']
keywords = '|'.join(keyword_list)

# 사전규격
df_pre = pd.read_sql(f"SELECT DISTINCT * "
                     f"FROM 수집_사전규격 "
                     f"WHERE 품명 REGEXP '{keywords}'"
                     f"ORDER BY 등록일시 DESC", con=engine)
# https://www.g2b.go.kr:8082/ep/preparation/prestd/preStdDtl.do?preStdRegNo=1328549

# 입찰공고
df_bid = pd.read_sql(f"SELECT DISTINCT 입찰공고번호, 입찰공고명, 배정예산금액, 추정가격, 입찰공고일시, "
                                     f"입찰마감일시, 개찰일시, 용역구분명, 공동수급구성방식명, 입찰공고상세URL "
                     f"FROM 수집_입찰공고 "
                     f"WHERE 입찰공고명 REGEXP '{keywords}' AND 공고종류명 !='취소'"
                     f"ORDER BY 입찰공고일시 DESC", con=engine)
# 낙찰내역
df_who = pd.read_sql(f"SELECT DISTINCT * "
                     f"FROM 수집_낙찰내역 "
                     f"WHERE 입찰공고명 REGEXP '{keywords}'"
                     f"ORDER BY 등록일시 DESC", con=engine)
#%%