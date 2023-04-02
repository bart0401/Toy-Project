# -*- coding: utf-8 -*-

# package
import os
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import leafmap.foliumap as leafmap
#%%
# Geodataframe
Tagert = gpd.read_file('Target_restaurant.gpkg',encoding='cp949')
Tagert.set_crs(epsg=5179, inplace=True)
Tagert.to_crs(epsg=4326, inplace=True)
Tagert.drop(columns=['element_type', 'osmid'], inplace=True)
Tagert['homepage'] = 'https://blog.naver.com/toryga'

road = gpd.read_file('road.gpkg',encoding='cp949')
road = road[['geometry']]

gpkg_list = [x for x in os.listdir() if 'buffer' in x]
buffer_list = []
for j in gpkg_list:
    temp = gpd.read_file(j,encoding='cp949')
    temp['speed'] = j.split('_')[0]+'km/h'
    temp['time'] = j.split('_')[1]+'minutes'
    buffer_list.append(temp)
base = gpd.read_file('base.gpkg',encoding='cp949')


# save to html
m = leafmap.Map(center = [37.50469, 127.11985],
                zoom = 6,
                layers_control=True,
                measure_control=False,
                tiles='https://map.pstatic.net/nrb/styles/basic/{z}/{x}/{y}.png',
                attr='ⓒNAVER'
                )

m.add_gdf(Tagert, layer_name = '대상')

# https://leafmap.org/leafmap/#leafmap.leafmap.Map.add_gdf : color parameter is not working
m.add_gdf(buffer_list[4], layer_name = '보행(4km/h, 소요시간 10분)', fill_colors=["red", "green", "blue"])
m.add_gdf(buffer_list[5], layer_name = '보행(4km/h, 소요시간 15분)', fill_colors=["red", "green", "blue"])
m.add_gdf(buffer_list[0], layer_name = '자전거(15km/h, 소요시간 10분)', fill_colors=["red", "green", "blue"])
m.add_gdf(buffer_list[1], layer_name = '자전거(15km/h, 소요시간 15분)', fill_colors=["red", "green", "blue"])
m.add_gdf(buffer_list[2], layer_name = '오토바이(30km/h, 소요시간 10분)', fill_colors=["red", "green", "blue"])
m.add_gdf(buffer_list[3], layer_name = '오토바이(30km/h, 소요시간 15분)', fill_colors=["red", "green", "blue"])

m.add_gdf(road, layer_name = '도로망', fill_colors=["red", "green", "blue"])

m.to_html('Visualize results.html')
#%%