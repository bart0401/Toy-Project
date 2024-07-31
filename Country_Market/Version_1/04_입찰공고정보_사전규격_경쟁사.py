# -*- coding: utf-8 -*-

class market:
    def __init__(self):
        print('시작합니다')

    def market_bid(self):

        # package
        import pandas as pd
        import json
        import time
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
        #------------------------------------------------------------------------------------------------------------
        MyServiceKey = ""
        path = ''
        os.chdir(path)
        before_data_key = [x for x in os.listdir() if 'key' in x][-1]
        #------------------------------------------------------------------------------------------------------------
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
                time.sleep(1)
            return table
        #------------------------------------------------------------------------------------------------------------
        if datetime.today().weekday() == 0:
            startdays = 3
        else:
            startdays = 1

        now = datetime.now() # 오늘
        end = now - relativedelta (days=1) # 1일 전
        end = end.strftime('%Y%m%d')
        start = now - relativedelta (days=startdays)
        start = start.strftime('%Y%m%d')
        #------------------------------------------------------------------------------------------------------------
        raw_data = country_market(start, end)
        #------------------------------------------------------------------------------------------------------------
        # df
        df_market = raw_data.copy().reset_index(drop=True)
        df_market.drop(columns=['rgnLmtBidLocplcJdgmBssCd', 'rgnLmtBidLocplcJdgmBssNm'],inplace=True)
        df_col = pd.read_excel(fr'{path}/입찰공고조회 칼럼.xlsx',header=None)
        df_market.columns = df_col[1].to_list()

        df_market = df_market.loc[~df_market['공고종류명'].str.contains('취소')==True]
        df_market.sort_values(by=["입찰공고일시"], ascending=[False], inplace=True)
        df_market.drop_duplicates(['입찰공고번호'], keep='first', inplace=True)
        df_market.drop_duplicates(['입찰공고명'], keep='first', inplace=True)
        #------------------------------------------------------------------------------------------------------------
        # keyword
        keyword_list = ['성장관리계획','성장관리방안','데이터기반','딥러닝','머신러닝','기계학습','인공지능',
                        '인구소멸','지방소멸','인구활력','인구감소','인구정책','인구시책','활성화계획','인구','탄소중립','인구활력',
                        '상권정보', '상권분석', '점포개발', '입지분석', '입지개발', 'CRM', '유통분석', 'GIS','공간분석',
                        '전세사기', '소유자정보', '주택시세', '부동산데이터', '부동산시세', '부동산AI',
                        '빅데이터', '알고리즘', 'AI분석', 'AI', '부동산빅데이터', '부동산입지',
                        '기업은행', '농협은행', '소상공인', '국방부', '부동산원','농림축산','농림','축산']
        market_list = []
        for k in keyword_list:
            temp = df_market.loc[df_market['입찰공고명'].apply(lambda x: x.replace(" ", "")).str.contains(k)==True]
            if startdays == 1:
                temp['수집기간'] = fr'{start}'
            else:
                temp['수집기간'] = fr'{start}~{end}'
            temp['키워드'] = k
            market_list.append(temp)
        df_market2 = pd.concat(market_list)

        # df frame
        df_market3 = df_market2.copy()
        df_market3.drop_duplicates(['입찰공고번호'], keep='first', inplace=True)
        df_market3.drop_duplicates(['입찰공고명'], keep='first', inplace=True)
        df_market3.sort_values(by=["키워드", "입찰공고일시"], ascending=[False, False], inplace=True)
        #------------------------------------------------------------------------------------------------------------
        # 전차 + 금회
        df_that = pd.read_excel(fr'{path}/{before_data_key}')
        df_this = pd.concat([df_market3, df_that])
        use_cols = df_that.columns.tolist() + ['공고기관명', '입찰참가자격등록마감일시','입찰마감일시']
        df_this = df_this[use_cols]
        df_this = df_this.drop_duplicates()
        #------------------------------------------------------------------------------------------------------------
        # bid_datail
        start9 = pd.Timestamp.now()
        # period
        market_list = df_this[~df_this['입찰공고번호'].isnull()]['입찰공고번호'].tolist()
        market_list = [x for x in market_list if (x.startswith((now-relativedelta(months=1)).strftime('%Y%m'))|x.startswith(now.strftime('%Y%m')))]
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
        if 0 in bid_datail.columns:
            bid_datail.drop(columns=[0, 1, 2, 3],inplace=True)
        else:
            pass
        bid_datail.columns = list(map(lambda x: x+str(1),bid_datail.columns.tolist()))
        bid_datail.rename(columns={'입찰공고번호1':'입찰공고번호'}, inplace = True)

        # bid_datail update
        if bid_datail.shape[0] != 0:
            df_this.fillna('', inplace=True)
            df_market4 = pd.merge(df_this, bid_datail, on='입찰공고번호', how='left')
            df_market4.rename(columns={'추첨 번호1': '추첨번호1'}, inplace=True)

            k_cols = ['업체명', '대표자명', '입찰금액(원)', '투찰률(%)', '추첨번호', '투찰일시', '개찰완료 조회가능 여부']
            for k in k_cols:
                df_market4[k] = np.where(df_market4[k] == '', df_market4[k + '1'], df_market4[k])
            df_market4['개찰완료 조회가능 여부'].fillna('불가', inplace=True)

            numeric_cols = ['배정예산금액', '추정가격', '입찰금액(원)', '투찰률(%)']
            for col in numeric_cols:
                df_market4[col] = pd.to_numeric(df_market4[col], errors='ignore')
            df_market4['추정가격'] = df_market4['추정가격'].fillna(0).astype('int64')

            df_market4['배정예산금액'] = pd.to_numeric(df_market4['배정예산금액'], errors='coerce').fillna(0).astype('int64')
            df_market4 = df_market4.fillna('')

            # Save for key
            df_market4.drop(columns=['순위1', '사업자등록번호1', '업체명1', '대표자명1', '입찰금액(원)1',
                                    '투찰률(%)1', '추첨번호1', '투찰일시1', '비고1', '개찰완료 조회가능 여부1'], inplace=True)
            df_market4 = df_market4[
                ['수집기간', '키워드', '입찰공고번호', '입찰공고일시', '참조번호', '입찰공고명', '공고기관명', '입찰방식명', '계약체결방법명', '개찰일시', '배정예산금액',
                 '추정가격', '용역구분명', '공동수급구성방식명', '낙찰방법명', '입찰공고상세URL', '공고규격서URL1', '공고규격서URL2', '공고규격서URL3', '공고규격서URL4',
                 '공고규격서URL5',
                 '공고규격파일명1', '공고규격파일명2', '공고규격파일명3', '공고규격파일명4', '공고규격파일명5', '업체명', '대표자명', '입찰금액(원)', '투찰률(%)', '추첨번호',
                 '투찰일시',
                 '비고', '개찰완료 조회가능 여부', '입찰참가자격등록마감일시', '입찰마감일시']]
            if startdays == 1:
                df_market4.to_excel(fr'{path}/조달청_입찰공고정보_{start}_key.xlsx', index=False)
            else:
                df_market4.to_excel(fr'{path}/조달청_입찰공고정보_{start}~{end}_key.xlsx', index=False)

            #  Save for Reporting
            df_market5 = df_market4[['수집기간', '키워드', '입찰공고일시', '입찰공고명', '개찰일시', '배정예산금액', '추정가격', '용역구분명',
                                    '공동수급구성방식명', '업체명', '대표자명', '입찰금액(원)', '투찰률(%)', '추첨번호', '투찰일시', '비고','입찰공고상세URL','공고기관명', '입찰참가자격등록마감일시','입찰마감일시']]
            if startdays == 1:
                df_market5.to_excel(fr'{path}/조달청_입찰공고정보_{start}.xlsx', index=False)
            else:
                df_market5.to_excel(fr'{path}/조달청_입찰공고정보_{start}~{end}.xlsx', index=False)

        else:
            print('')
    # ------------------------------------------------------------------------------------------------------------
    def market_pre(self):
        """
        ■ 사전규격정보 작업 시작 : 23.06.16 → 용역 목록 조회
        [ 오퍼레이션(영문) ]
        사전규격 물품 목록 조회 : getPublicPrcureThngInfoThng
        사전규격 외자 목록 조회 : getPublicPrcureThngInfoFrgcpt
        사전규격 용역 목록 조회 : getPublicPrcureThngInfoServc
        사전규격 공사 목록 조회 : getPublicPrcureThngInfoCnstwk
        """
        # package
        import pandas as pd
        import json
        import time
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
        pd.set_option('mode.chained_assignment', None)
        #------------------------------------------------------------------------------------------------------------
        # input
        MyServiceKey = ""
        path = ''
        os.chdir(path)

        #------------------------------------------------------------------------------------------------------------
        def country_market_pre(start, end, category):
            url_base = fr"http://apis.data.go.kr/1230000/HrcspSsstndrdInfoService/{category}?"
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

        #------------------------------------------------------------------------------------------------------------
        if datetime.today().weekday() == 0:
            startdays = 3
        else:
            startdays = 1

        now = datetime.now()  # 오늘
        end = now - relativedelta(days=1)  # 1일 전
        end = end.strftime('%Y%m%d')

        start = now - relativedelta(days=startdays)
        start = start.strftime('%Y%m%d')
        #------------------------------------------------------------------------------------------------------------
        # category_list = ['getPublicPrcureThngInfoThng', 'getPublicPrcureThngInfoFrgcpt', 'getPublicPrcureThngInfoServc', 'getPublicPrcureThngInfoCnstwk']
        category_list = ['getPublicPrcureThngInfoServc']
        raw_data_list = []

        for category in tqdm(category_list):
            temp = country_market_pre(start, end, category)
            raw_data_list.append(temp)

        raw_data = pd.concat(raw_data_list)
        #------------------------------------------------------------------------------------------------------------
        # df
        df_market = raw_data.copy().reset_index(drop=True)
        df_col = pd.read_excel(fr'{path}/사전규격 용역목록조회 칼럼.xlsx', header=None)
        df_market.columns = df_col[1].to_list()

        df_market = df_market[['품명', '발주기관명', '실수요기관명', '배정예산금액', '접수일시', '담당자명', '담당자전화번호']]
        #------------------------------------------------------------------------------------------------------------
        # keyword
        keyword_list = ['성장관리계획', '성장관리방안', '데이터기반', '딥러닝', '머신러닝', '기계학습', '인공지능',
                        '인구소멸', '지방소멸', '인구활력', '인구감소', '인구정책', '인구시책', '활성화계획', '인구', '탄소중립', '인구활력',
                        '상권정보', '상권분석', '점포개발', '입지분석', '입지개발', 'CRM', '유통분석', 'GIS', '공간분석',
                        '전세사기', '소유자정보', '주택시세', '부동산데이터', '부동산시세', '부동산AI',
                        '빅데이터', '알고리즘', 'AI분석', 'AI', '부동산빅데이터', '부동산입지',
                        '기업은행', '농협은행', '소상공인', '국방부', '부동산원', '농림축산', '농림', '축산', '플랫폼']
        market_list = []
        for k in keyword_list:
            temp = df_market.loc[df_market['품명'].apply(lambda x: x.replace(" ", "")).str.contains(k) == True]
            if startdays == 1:
                temp['수집기간'] = fr'{start}'
            else:
                temp['수집기간'] = fr'{start}~{end}'
            temp['키워드'] = k
            market_list.append(temp)
        df_market2 = pd.concat(market_list)
        df_market2['배정예산금액'] = df_market2['배정예산금액'].astype(np.int64)
        df_market2 = df_market2[['수집기간', '키워드', '품명', '발주기관명', '배정예산금액', '접수일시', '담당자명', '담당자전화번호']]

        before_data = pd.read_excel(fr'{path}/사전규격/' + [x for x in os.listdir(fr'{path}/사전규격/') if 'xlsx' in x][-1],
                                    sheet_name='사전규격')
        df_market2 = pd.concat([df_market2, before_data])
        df_market2 = df_market2.drop_duplicates(subset=['품명'])
        #------------------------------------------------------------------------------------------------------------
        # 시트별 저장
        before_data_key = [x for x in os.listdir() if 'key' not in x and 'xlsx' in x][-1]
        df_that = pd.read_excel(fr'{path}/{before_data_key}')
        df_open = df_that.copy()
        df_open = df_open[['수집기간', '키워드', '용역구분명', '입찰공고일시', '입찰공고명','공고기관명', '입찰참가자격등록마감일시','입찰마감일시', '개찰일시', '배정예산금액', '추정가격',
                                            '공동수급구성방식명', '업체명', '대표자명', '입찰금액(원)', '투찰률(%)', '추첨번호', '투찰일시', '비고','입찰공고상세URL']]
        open_mate = pd.read_excel(path+'오픈메이트_고객사.xlsx',header=None)
        df_open['공고기관명'].fillna('',inplace=True)
        open_str = '|'.join(open_mate[0].tolist())
        df_open = df_open[df_open['공고기관명'].str.contains(open_str)]
        writer = pd.ExcelWriter(fr'{path}/사전규격/' + before_data_key[:10] + '_사전규격' + before_data_key[10:],
                                engine='openpyxl')
        df_that.to_excel(writer, sheet_name='입찰공고', index=False)
        df_market2.to_excel(writer, sheet_name='사전규격', index=False)
        df_open.to_excel(writer, sheet_name='오픈메이트고객사', index=False)
        writer.save()
        #------------------------------------------------------------------------------------------------------------
        # xlsx to slack
        from slack_sdk import WebClient
        client = WebClient(token='')

        import os
        os.chdir(fr'{path}/사전규격/')
        now_data = [x for x in os.listdir()][-1]

        response = client.files_upload(
            channels="#country_market",
            file=now_data,
            text=now_data,
            title=now_data)
        print('끝났습니다')
#%%
# 클래스 실행
market().market_bid()
market().market_pre()
#%%
