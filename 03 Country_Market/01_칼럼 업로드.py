# -*- coding: utf-8 -*-

"""
■ 입찰공고, 사전규격, 낙찰내역 칼럼
■ 경쟁업체 고객사
"""
# package
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("postgresql://postgres:1234@localhost:5432/country_market", isolation_level='AUTOCOMMIT')
#%%
# 통합칼럼 생성
df1 = pd.read_excel('입찰공고조회 칼럼.xlsx',header=None)
df1.columns = ['영문칼럼','한글칼럼']
df1['칼럼종류'] = '입찰공고'

df2 = pd.read_excel('사전규격 용역목록조회 칼럼.xlsx',header=None)
df2.columns = ['영문칼럼','한글칼럼']
df2['칼럼종류'] = '사전규격'

df3 = pd.read_excel('낙찰 칼럼.xlsx',header=None)
df3.columns = ['영문칼럼','한글칼럼']
df3['칼럼종류'] = '낙찰내역'

df = pd.concat([df1,df2,df3],axis=0)
#------------------------------------------------------------
# 경쟁업체 고객사
df4 = pd.read_excel('경쟁업체_고객사.xlsx',header=None)
df4.columns = ['고객사명']
#%%
# db업로드
df.to_sql(name='칼럼_나라장터', con=engine, if_exists='append', index=False)
df4.to_sql(name='경쟁_고객사', con=engine, if_exists='append', index=False)
#%%
