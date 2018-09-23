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
    #centermost_point = min(cluster, key=lambda point: (point.all(), centroid))
    return centroid

def dbscan_reduce_afvalcontainers(df, fractie=None):
    
    start_time = time.time()
    
    coords = df[['y', 'x']].values
    print ('shape input frame afval_full : {}'.format(df.shape))
    db = (DBSCAN(**yml['dbscan_afvalcontainers']['params']).fit(coords))

    cluster_labels = db.labels_
    print ('shape cluster_labels array: {}'.format(cluster_labels.shape))
    num_clusters = len(set(cluster_labels))
    print ('num_clusters: {}'.format(num_clusters))
    
    cluster_frame = pd.DataFrame(cluster_labels).rename(columns = {0: 'cluster_toewijzing'})
    
    df = df.join(cluster_frame)
    
    # create set with cluster centroids and cluster toewijzing
    coords_cluster = pd.DataFrame([n for n in range(num_clusters)])
    coords_cluster = coords_cluster.rename(columns={0 : 'cluster_toewijzing'})
    
    cluster_array = pd.Series([coords[cluster_labels==n] for n in range(num_clusters)])
    centroids = cluster_array.map(get_centroid)
    lats, lons = zip(*centroids)
    
    rep_points = pd.DataFrame({'x_centroid':lons, 'y_centroid':lats})
    
    rep_points['cl_geom'] = (rep_points.apply(lambda row: Point(row["x_centroid"], 
                                                                row["y_centroid"]), 
                                              axis=1))
    
    afval_clusters = rep_points.join(coords_cluster)
    
    keep_cols = ['cl_geom', 'cluster_toewijzing']
    df = pd.merge(df, afval_clusters[keep_cols], on= ['cluster_toewijzing'], how='left')
    
    for drop_cols in ['x', 'y']:
        df = df.drop(drop_cols, axis=1)
    
    df = df.rename(columns = {'geometry': 'point_geom', 'cl_geom': 'geometry'})
    
    if fractie:
        df_fractie = (df[df.fractie == fractie]
                      .reset_index(drop=True)
                      .drop_duplicates(subset=['cluster_toewijzing']))
        
        print ("Clustered {:,} afvalcontainers down to {:,} inzamellocaties, for {:.2f}% compression in {:,.2f} sec.".format(
                   len(df), len(df_fractie), 100*(1 - float(len(df_fractie)) / len(df)), time.time()-start_time))
               
        return df_fractie
    
    print ("Clustered {:,} afvalcontainers down to {:,} inzamellocaties, for {:.2f}% compression in {:,.2f} sec.".format(
                   len(df), len(afval_clusters), 100*(1 - float(len(afval_clusters)) / len(df)), time.time()-start_time))
    
    return df


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


def get_afvalcontainers_full_df(column_subset=None):
    
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
    
    df = df.reset_index(drop=True)
    logger.info("index has been reset")
    logger.info("Afvalcontainers_full df has shape: {} and crs: {}".format(
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
        
    return df.reset_index(drop=True)


### some quick helpers to load in the stag_tables csvs and put back into geoframes
def get_stag_table_huur():
    df = pd.read_csv(yml['path']['data_stag_tables'] + yml['file_stag_tables']['clusters_huur'], dtype=str)
    df['geometry'] = df['geometry'].apply(lambda x: wkt.loads(x))
    df['pnd_geom'] = df['pnd_geom'].apply(lambda x: wkt.loads(x))
    df = gpd.GeoDataFrame(df, geometry = 'geometry', crs=yml['crs']['crs'])
    
    logger.info("GeoDataFrame has shape: {} and crs: {}".format(
                 df.shape, crs))
    
    return df

def get_stag_table_koop():
    df = pd.read_csv(yml['path']['data_stag_tables'] + yml['file_stag_tables']['clusters_koop'], dtype=str)
    df['geometry'] = df['geometry'].apply(lambda x: wkt.loads(x))
    df['pnd_geom'] = df['pnd_geom'].apply(lambda x: wkt.loads(x))
    df = gpd.GeoDataFrame(df, geometry = 'geometry', crs=yml['crs']['crs'])
    
    logger.info("GeoDataFrame has shape: {} and crs: {}".format(
                 df.shape, crs))
    
    return df

def get_stag_table18():
    df = pd.read_csv(yml['path']['data_stag_tables'] + yml['file_stag_tables']['clusters18'], dtype=str)
    df['geometry'] = df['geometry'].apply(lambda x: wkt.loads(x))
    df['pnd_geom'] = df['pnd_geom'].apply(lambda x: wkt.loads(x))
    df = gpd.GeoDataFrame(df, geometry = 'geometry', crs=yml['crs']['crs'])
    
    logger.info("GeoDataFrame has shape: {} and crs: {}".format(
                 df.shape, crs))
    
    return df

def get_stag_table65():
    df = pd.read_csv(yml['path']['data_stag_tables'] + yml['file_stag_tables']['clusters65'], dtype=str)
    df['geometry'] = df['geometry'].apply(lambda x: wkt.loads(x))
    df['pnd_geom'] = df['pnd_geom'].apply(lambda x: wkt.loads(x))
    df = gpd.GeoDataFrame(df, geometry = 'geometry', crs=yml['crs']['crs'])
    
    logger.info("GeoDataFrame has shape: {} and crs: {}".format(
                 df.shape, crs))
    
    return df


def get_distance_matrices(path, file):
    
    """
    load in the munged distance matrices resulting from function 
    distance_matrix/deduplicate_distance_matrix_.. 
    """
    df = pd.read_csv(path + file)
    geo_cols = ['geometry', 'geom_point']
    for col in df[geo_cols]:
        df[col] = df[col].apply(lambda x: wkt.loads(x))
    df = df.drop('_merge', axis=1)
    df = gpd.GeoDataFrame(df, geometry = 'geometry', crs=yml['crs']['crs'])
    
    logger.info("GeoDataFrame has shape: {} and crs: {}".format(
                 df.shape, crs))
    
    return df