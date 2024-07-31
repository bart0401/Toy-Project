# -*- coding: utf-8 -*-

# package
import os
import osmnx as ox
import networkx as nx
import pandana
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import alphashape
#%%
# Select city and crs
cityname = 'Songpa-gu, korea'
crs = 5179

# Get graph by geocoding
graph = ox.graph_from_place(cityname, network_type="walk")
# Project graph
graph = ox.projection.project_graph(graph, to_crs=crs)

# Select points of interest based on osm tags
tags = {'amenity': ['restaurant']}

# Get amentities from place
pois = ox.geometries.geometries_from_place(cityname, tags=tags)

# Project pois
pois = pois.to_crs(epsg=crs)
# Target
pois = pois[pois['name'].str.contains('토리가')==True]
pois[['name','amenity','geometry']].to_file('Target_restaurant.gpkg')
#%%
# speed(input value)
# walking = 4, bicycle = 15, motorcycle = 30
speed_list = [4, 15, 30]
for speed in tqdm(speed_list):
    # Set a uniform speed on every edge
    for u, v, data in graph.edges(data=True):
        data['speed_kph'] = speed
    graph = ox.add_edge_travel_times(graph)

    # Extract node/edge GeoDataFrames
    nodes = ox.graph_to_gdfs(graph, edges=False)
    edges = ox.graph_to_gdfs(graph, nodes=False)
    edges['travel_time(min)'] = edges['travel_time'] / 60

    G = ox.utils_graph.graph_from_gdfs(nodes, edges, graph_attrs={'travel_time': edges['travel_time(min)'], 'crs': 5179})
    target_node = ox.distance.nearest_nodes(G, float(pois.geometry[0].x), float(pois.geometry[0].y))

    moving_time_list = [10, 15]
    for moving_time in tqdm(moving_time_list):
        service_area = nx.ego_graph(G=G, n=target_node, radius=(speed*1000/60)*moving_time, distance='length')
        ego_nodes, ego_edges = ox.graph_to_gdfs(service_area)

        # save to file
        ego_nodes.to_file(fr'{speed}_{moving_time}_nodes.gpkg', encoding='cp949')
        ego_edges[['length','speed_kph','travel_time(min)','geometry']].to_file(fr'{speed}_{moving_time}_edges.gpkg', encoding='cp949')

        # buffer
        ego_nodes.set_crs(epsg=5179, inplace=True)
        ego_nodes.to_crs(epsg=4326, inplace=True)

        alpha_shape = alphashape.alphashape(ego_nodes, 500)
        alpha_shape.to_file(fr'{speed}_{moving_time}_buffer.gpkg', encoding='cp949')
#%%
# save to background file
graph = ox.graph_from_place('Songpa-gu, korea', network_type="walk")
ox.save_graph_geopackage(graph, filepath='./base.gpkg')
#%%