# -*- coding: utf-8 -*-

# package
from slack_sdk import WebClient
import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import urllib3
from sqlalchemy import create_engine
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
engine = create_engine("mysql+pymysql://root:1234@localhost:3306/country_market", isolation_level='AUTOCOMMIT')
#%%
# 키워드 목록
keyword_list = [
    '데이터', '딥러닝', '머신러닝', '기계학습', '인공지능', '알고리즘', '생활권', '기본계획', '관리계획', '성장관리'
                '인구','소멸','활성화계획','탄소','탄소중립', '상권', '점포', '입지', '유통', 'GIS','공간', '전세사기', '소유자',
                '주택', '부동산', '시세', '은행', '소상공인', '국방부', '부동산원', '농림', '축산', '감염', 'AI','CRM'
]
keywords = '|'.join(keyword_list)
#%%
# 사전규격
df_pre = pd.read_sql(f"SELECT DISTINCT * "
                     f"FROM 수집_사전규격 "
                     f"WHERE 품명 REGEXP '{keywords}'"
                     f"ORDER BY 등록일시 DESC", con=engine)
df_pre['사전규격URL'] = 'https://www.g2b.go.kr:8082/ep/preparation/prestd/preStdDtl.do?preStdRegNo=' + df_pre['사전규격등록번호']
df_pre['분석과업여부'] = df_pre['품명'].apply(lambda x: 1 if '분석' in x else 0)

# 입찰공고
df_bid = pd.read_sql(f"SELECT DISTINCT 입찰공고번호, 입찰공고명, 배정예산금액, 추정가격, 입찰공고일시, "
                                     f"입찰마감일시, 개찰일시, 용역구분명, 공동수급구성방식명, 입찰공고상세URL "
                     f"FROM 수집_입찰공고 "
                     f"WHERE 입찰공고명 REGEXP '{keywords}' AND 공고종류명 !='취소'"
                     f"ORDER BY 입찰공고일시 DESC", con=engine)
df_bid['분석과업여부'] = df_bid['입찰공고명'].apply(lambda x: 1 if '분석' in x else 0)

# 낙찰내역
df_who = pd.read_sql(f"SELECT DISTINCT * "
                     f"FROM 수집_낙찰내역 "
                     f"WHERE 입찰공고명 REGEXP '{keywords}'"
                     f"ORDER BY 등록일시 DESC", con=engine)
df_who['분석과업여부'] = df_who['입찰공고명'].apply(lambda x :1 if '분석' in x else 0)
#%%
# 파일저장시점 설정
yesterday = (datetime.now() - relativedelta(days=1)).strftime('%Y%m%d')
yesterday_type = (datetime.now() - relativedelta(days=1)).strftime('%Y-%m-%d')
#%%
# 데이터프레임을 엑셀파일로 저장
with pd.ExcelWriter(fr'조달청_서면보고_{yesterday}.xlsx', engine='xlsxwriter') as writer:
    df_pre.to_excel(writer, sheet_name= '사전규격')
    df_bid.to_excel(writer, sheet_name= '입찰공고')
    df_who.to_excel(writer, sheet_name= '낙찰내역')
#%%
# 슬랙(Slack) 메신저 활용
report_xlsx = [x for x in os.listdir() if '조달청_서면보고' in x][-1]

client = WebClient(token='')

# 파일 전송
response = client.files_upload(
    channels="#country_market",
    file=report_xlsx,
    text=report_xlsx,
    title=report_xlsx
)

# 메세지 전송
def send_message(df, column, column1, message):
    # df: dataframe, column: 시점칼럼, column1: 기준칼럼, message: 전달내용
    if df[df[column].str[:10]==yesterday_type][column1].sum() > 0:
        client.chat_postMessage(
        channels="#country_market",
        text=message)

send_message(df_pre, '접수일시', '분석과업여부', '낙찰내역 검토 필요')
send_message(df_bid, '입찰공고일시', '분석과업여부', '입찰공고 검토 필요')
#%%
