# -*- coding: utf-8 -*-

"""
최근린분석: sjoin_nearest
"""
#%%
#-----------------------------------
import geopandas as gpd
import os
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
pd.set_option('mode.chained_assignment',  None)
#-----------------------------------
#%%
# 경로설정
path = os.getcwd()
os.chdir(path + '/01 따릉이/')
#-----------------------------------
# 데이터 불러오기
station = pd.read_csv('서울시 역사마스터 정보.csv',encoding='cp949')
bike = pd.read_excel('공공자전거 대여소 정보(22.06월 기준).xlsx')
#-----------------------------------
# 데이터 구성 확인
station.info()
bike.info()

station.rename(columns = {'위도': 'x','경도': 'y'}, inplace = True)
bike.rename(columns = {'경도': 'x','위도': 'y'}, inplace = True)
#-----------------------------------
# 지오코딩: x,y좌표를 point의 Geodataframe으로 변경
def point_to_shp(data, data_x, data_y, input_epsg, output_epsg):
    temp = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data[data_x],data[data_y]))
    temp.set_crs(epsg=input_epsg, allow_override=True, inplace=True)
    temp.to_crs(epsg=output_epsg,inplace=True)
    return temp

gdf_station = point_to_shp(station,'x','y',4326,5181).drop(columns=['x','y'])
gdf_bike = point_to_shp(bike,'x','y',4326,5181).drop(columns=['x','y'])
#-----------------------------------
# 중복좌표 제거
gdf_station = gdf_station.drop_duplicates(['geometry'], keep='first')
gdf_bike = gdf_bike.drop_duplicates(['geometry'], keep='first')
#-----------------------------------
# 최근린분석(250m)
gdf_near = gpd.sjoin_nearest(gdf_station, gdf_bike, max_distance=500, distance_col='dist').drop(columns=['index_right'])
gdf_near.insert(len(gdf_near.columns) - 1, 'geometry', gdf_near.pop('geometry'))
gdf_near.info()
gdf_near['거치대수LCD'] = gdf_near['거치대수LCD'].fillna(0)
gdf_near['거치대수QR'] = gdf_near['거치대수QR'].fillna(0)
gdf_near.head()
#-----------------------------------