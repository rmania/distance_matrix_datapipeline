import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import wkt, wkb
from shapely.geometry import Polygon, Point
import sys
sys.path.insert(0, '../helper_functions/')
import wfs_processing as wfs

from logger_settings import logger
logger = logger()

# path
from pathlib import Path
PATH_BASE_POI = '../data/base_poi/'
PATH_POI2 = '../data/poi2/'
p1 = Path(PATH_BASE_POI)
p2 = Path(PATH_POI2)
# crs
crs= {'init': 'epsg:28992'}

# files
ah = 'ah_vestigingen.csv'
oba = 'oba_vestigingen.csv'


def get_df1(path, bag_file, see_file_options = bool, plot=bool):
    """
    load dataframe with the second poi geometries
    currently choice from ah, oba
    """
    # bag clusters
    df = pd.read_csv(path + bag_file, dtype=str)
    
    logger.info("Loading {} GeoDataFrame, with shape: {} and crs: {}".format(
        bag_file, df.shape, crs))
    
    df['geometry'] = df['cl_geom'].apply(lambda x: wkb.loads(x, hex=True))
    df = gpd.GeoDataFrame(df, crs= crs, geometry='geometry')
    df = df.drop('cl_geom', axis=1)
    
    if see_file_options:
        [print (i.name) for i in p1.rglob('*.csv')]
    
    if plot:
        n=1000
        fig, ax = plt.subplots(figsize=[15,7])
        logger.info("Plotting {} POINTS".format(n))
        df[:n].plot(ax=ax, color='blue', alpha=.5)
            
    return df


def get_df2(path, file, see_file_options = bool, plot=bool):
    
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
        
    if file == ah:
        buffer = 1000
        df['buffer'] = df['geometry'].buffer(buffer)
        logger.info("created {} meter buffer around {} geometry".format(
        buffer, df.geometry.geom_type[0]))
    else:
        buffer = 2000
        df['buffer'] = df['geometry'].buffer(buffer)
        logger.info("created {} meter buffer around {} geometry".format(
        buffer, df.geometry.geom_type[0]))

    if see_file_options:
        [print (i.name) for i in p2.rglob('*.csv')]
    
    if plot:
        fig, ax = plt.subplots(figsize=[15,7])
        ax = std.plot(ax=ax)
        df.plot(ax=ax, color='red', alpha=.5, marker = '*')
        gpd.GeoSeries(df.geometry.buffer(buffer)).plot(ax=ax, color='yellow', alpha=.085)
    return df