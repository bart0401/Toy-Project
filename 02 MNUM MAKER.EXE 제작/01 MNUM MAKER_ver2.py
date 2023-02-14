# -*- coding: utf-8 -*-

"""
■ 좌표변경 기능 추가
"""


# package
import os
import geopandas as gpd
import pandas as pd

# path
df_class = pd.read_excel(os.getcwd() + fr'/02 MNUM MAKER.EXE 제작/용도지역지구구분코드 조회자료.xlsx')

# input
source_path = os.getcwd() + fr'/02 MNUM MAKER.EXE 제작/source/'  # input path
save_path = os.getcwd() + fr'/02 MNUM MAKER.EXE 제작/save/'      # output path
epsg_orignal = 5174
epsg_change = 4326

# def
def MNUM_MAKER(source_path, save_path, epsg_orignal, epsg_change):
    shp_list = [s for s in os.listdir(source_path) if "shp" in s]
    for shp in shp_list:
        temp = gpd.read_file(source_path + shp, encoding='cp949')
        temp['SIGUNGU'] = temp['MNUM'].str[7:12]
        temp['CODE'] = temp['MNUM'].str[20:26]
        temp = temp.merge(df_class, how='left', on='CODE')
        temp['CLASS1'].fillna('확인필요',inplace=True)
        temp['CLASS2'].fillna('확인필요',inplace=True)
        temp.set_crs(epsg=epsg_orignal, allow_override=True, inplace=True)
        temp.to_crs(epsg=epsg_change, inplace=True)
        temp.insert(len(temp.columns) - 1, 'geometry', temp.pop('geometry'))
        temp['geometry'] = temp.buffer(0)
        temp.to_file(save_path+'MNUM_MAKER_'+shp, encoding='cp949')
#%%
MNUM_MAKER(source_path, save_path, epsg_orignal, epsg_change)
