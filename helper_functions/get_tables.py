import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import wkt, wkb
from shapely.geometry import Polygon, Point, shape, MultiPoint
import requests
from sklearn.cluster import DBSCAN
from pandas.io.json import json_normalize
import time

import sys
sys.path.insert(0, '../helper_functions/')
import wfs_processing as wfs

from logger_settings import logger
logger = logger()

import yaml
with open("../config.yaml", 'r') as config_yml:
    try:
        yml = yaml.load(config_yml)        
    except yaml.YAMLError as exc:
        print (exc)

# paths --
from pathlib import Path
p1 = Path(yml['path']['data_path_poi1'])
p2 = Path(yml['path']['data_path_poi2'])
crs= yml['crs']['crs']


### convencience fucntion to list available POI files
def list_poi_files():
    print (yml['file'])


### function to get other BAG FILE 
def get_df1(path, bag_file, add_brp_18 = None, add_brp_65 = None, plot=None):
    """
    load bag data with or w/o 18/65 additional info
    args:
        path = path to the data folder
        bag_file = bag_file. See yml['file'] for options
     
    """
    # bag clusters
    df = pd.read_csv(path + bag_file, dtype=str)
    
    logger.info("Loading {} GeoDataFrame, with shape: {} and crs: {}".format(
                bag_file, df.shape, crs))
    
    df['geometry'] = df['cl_geom'].apply(lambda x: wkb.loads(x, hex=True))
    df = gpd.GeoDataFrame(df, crs= yml['crs']['crs'], geometry='geometry')
    df = df.drop('cl_geom', axis=1)
    
    if bag_file == yml['file']['bag_full']:
        df['pnd_geom'] = df['pnd_geom'].apply(lambda x: wkb.loads(x, hex=True))
    
    if add_brp_18:
        if bag_file == yml['file']['bag_full']:
            
            vot18 = pd.read_csv(yml['path']['data_path_brp'] + yml['file']['vot18'], 
                            sep=';', dtype=str)
            vot18['18'] = 18
            df = pd.merge(df, vot18[['lv_bag_vot_id' ,'18']], 
                    left_on=['landelijk_vot_id'], 
                    right_on=['lv_bag_vot_id'], 
                    how='left', indicator=True)
            
            logger.info("Matched {} rows with left_only join".format(
                        df._merge.value_counts()[0]))
            
            return df        
        else:
            print ('add_brp_18 not applicable to bag_clusters dataset')
                       
    if add_brp_65:
        if bag_file == yml['file']['bag_full']:
            vot65 = pd.read_csv(yml['path']['data_path_brp'] + yml['file']['vot65'], 
                            sep=';', dtype=str)
            vot65['65'] = 65
            df = pd.merge(df, vot65[['lv_bag_vot_id' ,'65']], 
                    left_on=['landelijk_vot_id'], 
                    right_on=['lv_bag_vot_id'], 
                    how='left', indicator=True)
            
            logger.info("Matched {} rows with left_only join".format(
                        df._merge.value_counts()[0]))            
            return df        
        else:
            print ('add_brp_65 not applicable to bag_clusters dataset')
    
    if plot:
        n=1000
        fig, ax = plt.subplots(figsize=[15,7])
        logger.info("Plotting {} POINTS".format(n))
        df[:n].plot(ax=ax, color='blue', alpha=.5)
            
    return df

### FUNCTIONS for clustering afvalcontainers (POI set 2)
def get_centroid(cluster):
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: (point.all(), centroid))
    return tuple(centermost_point)

def dbscan_reduce_afvalcontainers(df, x='x', y='y'):
    start_time = time.time()
    # matrix of np arrays 
    coords = df[['y', 'x']].values
    db = (DBSCAN(**yml['dbscan_afvalcontainers']['params']).fit(coords))
    
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
       
    clusters = pd.Series([coords[cluster_labels==n] for n in range(num_clusters)])
    
    # find point in each cluster closest to its centroid
    centermost_points = clusters.map(get_centroid)

    # unzip list of centermost points (lat, lon) 
    lats, lons = zip(*centermost_points)
    rep_points = pd.DataFrame({x:lons, y:lats})
       
    rs = rep_points.apply(lambda row: df[(df[y]==row[y]) & (df[x]==row[x])].iloc[0], axis=1)
    rs = gpd.GeoDataFrame(rs, geometry = 'geometry', crs = yml['crs']['crs'])
    
    logger.info("Clustered {:,} afvalcontainers down to {:,} inzamellocaties, for {:.2f}% compression in {:,.2f} sec.".format(
                   len(df), len(rs), 100*(1 - float(len(rs)) / len(df)), time.time()-start_time))
    
    return rs


def dbscan_reduce_vot(df, x='x', y='y'):
    start_time = time.time()
    # matrix of np arrays 
    coords = df[['y', 'x']].values
    db = (DBSCAN(**yml['dbscan_vot']['params']).fit(coords))
    
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
       
    clusters = pd.Series([coords[cluster_labels==n] for n in range(num_clusters)])
    
    # find point in each cluster closest to its centroid
    centermost_points = clusters.map(get_centroid)

    # unzip list of centermost points (lat, lon) 
    lats, lons = zip(*centermost_points)
    rep_points = pd.DataFrame({x:lons, y:lats})
       
    rs = rep_points.apply(lambda row: df[(df[y]==row[y]) & (df[x]==row[x])].iloc[0], axis=1)
    rs = gpd.GeoDataFrame(rs, geometry = 'geometry', crs = yml['crs']['crs'])
    
    logger.info("Clustered {:,} verblijfsobjecten down to {:,} vot_clusters, for {:.2f}% compression in {:,.2f} sec.".format(
                   len(df), len(rs), 100*(1 - float(len(rs)) / len(df)), time.time()-start_time))
    
    return rs


def get_afvalcontainers_df(column_subset=None, dbscan_clustering=None):
    
    # load geojson
    params = yml['afvalcontainers']['params']
    url = yml['afvalcontainers']['url']
    response = requests.get(url=url, params=params)
    data = response.json()
    
    # parse the json, give nice names
    results = []

    for item in data['features']:
        result_dict = {}

        result_dict['geometry'] = item['geometry']['coordinates']
        result_dict['active'] = item['properties']['active']
        result_dict['buurt_code'] = item['properties']['buurt_code']
        result_dict['container_type_id'] = item['properties']['container_type_id']
        result_dict['container_id'] = item['properties']['id']
        result_dict['id_number'] = item['properties']['id_number']
        result_dict['operate_date'] = item['properties']['operational_date']
        result_dict['owner'] = item['properties']['owner']
        result_dict['place_date'] = item['properties']['placing_date']
        result_dict['serial_number'] = item['properties']['serial_number']
        result_dict['stadsdeel'] = item['properties']['stadsdeel']
        result_dict['address'] = item['properties']['text']
        result_dict['fractie'] = item['properties']['waste_name']
        result_dict['fractie_type'] = item['properties']['waste_type']

        results.append(result_dict)
        
    df = gpd.GeoDataFrame(results, crs = yml['crs']['crs_4326'])
    
    # filter out messy fracties/ waste_types
    df = df[(df['fractie'].isin(yml['afvalcontainers']['fracties']))]
    
    # convert geometry column to Points
    df['geometry'] = [Point(xy) for xy in df['geometry']]

    #flatten the 'owner column, merge back on df, drop owner column
    owner = json_normalize(df['owner'])
    df = (pd.merge(df, owner, left_index=True, right_index=True)
            .drop(labels=['owner'], axis=1))
    
    if column_subset:
        keep_cols = ['container_id', 'geometry', 'fractie']
        df = df[keep_cols]        
    
    # to crs 28992
    df = df.to_crs(crs=yml['crs']['crs'])
    # filter an annoying outlier
    df['x'] = df['geometry'].x
    df['y'] = df['geometry'].y
    
    df = df[((df.x >= 110000) & (df.x <= 135000) & 
             (df.y >= 475000) & (df.y <= 494000))]
    
    if dbscan_clustering:
        df_clustered = dbscan_reduce(df=df)
        
        logger.info("Clustered GeoDataFrame has shape: {} and crs: {}".format(
                   df_clustered.shape, df_clustered.crs))
        
        return df_clustered
    
    logger.info("Non-clustered GeoDataFrame has shape: {} and crs: {}".format(
                 df.shape, crs))
    
    return df


### Functions to get other POI2 dataframes to calculate distance from
def get_df2(path, file, plot=bool):
    
    """
    load dataframe with the second poi geometries
    currently choice from ah, oba
    """
    df = pd.read_csv(path + file, dtype=str)
    
    logger.info("Loading {} GeoDataFrame, with shape: {} and crs: {}".format(
        file, df.shape, crs))
    
    if df.columns.str.contains('geom').any():
        df = df.rename(columns = {'geom': 'geometry'})
    
    df['geometry'] = df['geometry'].apply(lambda x: wkb.loads(x, hex=True))
    df = gpd.GeoDataFrame(df, crs= crs, geometry='geometry')
    
    # join stadsdelen on poi2
    std = wfs.get_sd_layer()
    df = gpd.sjoin(df, std, how='inner', op = 'intersects')
    logger.info("Spatial join of {} GeoDataFrame and Amsterdam district layer. \
    Added columns : {}".format(file, std.columns.tolist()))
        
    if file == yml['file']['ah']:
        buffer = 1000
        df['buffer'] = df['geometry'].buffer(buffer)
        logger.info("created {} meter buffer around {} geometry".format(
        buffer, df.geometry.geom_type[0]))
    else:
        buffer = 2000
        df['buffer'] = df['geometry'].buffer(buffer)
        logger.info("created {} meter buffer around {} geometry".format(
        buffer, df.geometry.geom_type[0]))

    if plot:
        fig, ax = plt.subplots(figsize=[15,7])
        ax = std.plot(ax=ax)
        df.plot(ax=ax, color='red', alpha=.5, marker = '*')
        gpd.GeoSeries(df.geometry.buffer(buffer)).plot(ax=ax, color='yellow', alpha=.085)
    return df