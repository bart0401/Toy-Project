# -*- coding: utf-8 -*-

# package
import os
import folium
import geopandas as gpd
from geopandas.explore import _categorical_legend

import pyproj
pyproj_path = os.path.join(os.path.dirname(pyproj.__file__), 'proj_dir', 'share', 'proj')
pyproj.datadir.set_data_dir(pyproj_path)
os.environ['PROJ_LIB'] = pyproj_path
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
#%%
# Visualize
m = road.explore(color='gray',tiles='https://map.pstatic.net/nrb/styles/basic/{z}/{x}/{y}.png', attr='ⓒNAVER',name="도로망",tooltip=False)
buffer_list[3].explore(m=m, color='purple',name="오토바이(30km/h, 소요시간 15분)",tooltip=True,style_kwds=dict(fillOpacity=0.2),legend=True)
buffer_list[2].explore(m=m, color='blue',name="오토바이(30km/h, 소요시간 10분)",tooltip=True,style_kwds=dict(fillOpacity=0.2))
buffer_list[1].explore(m=m, color='green',name="자전거(15km/h, 소요시간 15분)",tooltip=True,style_kwds=dict(fillOpacity=0.2))
buffer_list[0].explore(m=m, color='yellow',name="자전거(15km/h, 소요시간 10분)",tooltip=True,style_kwds=dict(fillOpacity=0.2))
buffer_list[5].explore(m=m, color='orange',name="보행(4km/h, 소요시간 15분)",tooltip=True,style_kwds=dict(fillOpacity=0.2))
buffer_list[4].explore(m=m, color='red',name="보행(4km/h, 소요시간 10분)",tooltip=True,style_kwds=dict(fillOpacity=0.2))
Tagert.explore(m=m, color='Target',marker_kwds=dict(radius=7, fill=True),name="타겟(사장님)",tooltip=True)

_categorical_legend(
    m,
    title="Last_Mile_Delivery Service_Area",
    categories=[
        "타겟(사장님)",
        "보행(4km/h, 소요시간 10분)",
        "보행(4km/h, 소요시간 15분)",
        "자전거(15km/h, 소요시간 10분)",
        "자전거(15km/h, 소요시간 15분)",
        "오토바이(30km/h, 소요시간 10분)",
        "오토바이(30km/h, 소요시간 15분)",
        "도로망"],
    colors=['black','red','orange','yellow','green','blue','purple','gray'])

folium.LayerControl(control=True).add_to(m)  # use folium to add layer control

m.save('Visualize results.html')
#%%