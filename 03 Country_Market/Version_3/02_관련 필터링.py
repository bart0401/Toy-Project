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
df = pd.read_sql(f"SELECT * FROM country_market.분석_낙찰내역 where 입찰공고명 regexp '용역|사업' AND 입찰공고명 "
                 f"NOT regexp '유지보수|분석센터|분석모델 개발사업|모델개발 사업|재구축|플랫폼 구축|시스템 구축|ISMP|측량|국가 바이오|서비스 고도화|시스템 고도화|컨설팅|감리|운영|NGS|FEMS|UAV|인사관리|유지관리|국가바이오|위탁|직무교육|제작|연구|역량|ISP|기능 강화|엔지니어링SW'", con=engine)
df1 = df[['입찰공고번호','입찰공고명','최종낙찰업체명','최종낙찰금액', '최종낙찰률','수요기관명','실개찰일시']]
