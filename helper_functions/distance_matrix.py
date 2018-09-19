## packages
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import geopandas as gpd
import shapely.speedups
shapely.speedups.enable()
from shapely.ops import nearest_points
from logger_settings import logger
logger = logger()
from tqdm import tqdm, tqdm_notebook

# ========================================
##  ---------base functions
def calculate_distance(row, dest_geom, src_col='geometry', 
                      target_col='distance'):
    """
    Calculates distance between single Point geometry and GeoDF 
    with Point geometries.

    Parameters
    ----------
    dest_geom : shapely.Point
        single Shapely Point geometry to which distances will be calculated to.
    src_col : str
        column with Point objects from where the distances will be calculated from.
    target_col : str
        name of target column where the result will be stored.
    """
    # Calculate the distances
    dist = row[src_col].distance(dest_geom)
    # Tranform into kilometers
    dist_km = dist/1000
    # Assign the distance to the original data
    row[target_col] = dist_km
    
    return row



def find_nearest_point(row, geom_union, df1, df2, geom1_col='geometry', 
            geom2_col='geometry', src_column=None):
    """Find nearest point,return corresponding value from specified column
    Parameters
    ----------
    geom_union = variable with unary union of points from second frame. 
        Create with df2.unary_union
    df1 = dataframe one containing geometry column (points)
    df2 = dataframe two containing geometry column (points)
    geom1_col = geometry column name from df1
    geom2_col = geometry column name from df2
    src_column =  columns from df2 to be retrieved based on nearest match 
    """
    # Find closest geometry
    nearest = df2[geom2_col] == nearest_points(row[geom1_col], geom_union)[1]
    # Get corresponding value from df2 (based on geometry match)
    value = df2[nearest][src_column].get_values()[0]
    
    return value

# ========================================
# specific distance_matrix_afval functions
def create_distance_matrix_afval(df1, df2, fractie = str, buffer = int, 
                                 include_nearest_point = None, n=int):
    """
    calculate distance matrix frames. See ../helper_functions/distance_matrix.py 
    Make sure the you feed geoPandas df with a geometry column
    args:
        df1 : dataframe one containing geometry column (points)
        df2 : dataframe  with clustered afvalcontainers 
              containing geometry column (points)
        buffer: buffer in meters around the geometry column in df2 
        include_nearest_point: Find nearest point,return corresponding value 
        from specified column.Caution very slow on big sets.
        n : number of iterations. set n=len(df2) to loop through full set
    """
    df2 = df2[df2['fractie'] == fractie].reset_index()
    df2['buffer'] = df2['geometry'].buffer(buffer)
    n= n
    
    logger.info('Building dm (fractie: {}, buffer: {} with {} iterations' \
                .format(fractie, buffer, n))
    
    stag_distance = []
    
    for i, row in enumerate(tqdm_notebook(list(df2['buffer'][:n]))):
        sub_df = df1.loc[(df1.geometry.within(df2['buffer'][i])), :]
        sub_df = (sub_df.apply(calculate_distance, 
                               dest_geom= df2['geometry'][i], 
                     target_col= 'distance', axis=1))
        print ('Shape sub_df {} = {}'.format(i, sub_df.shape))
        
        if include_nearest_point:
            indices = (sub_df.apply(find_nearest_point, 
                          geom_union=df2.unary_union, 
                          df1=sub_df, 
                          df2=df2, 
                          geom1_col='geometry', 
                          src_column='container_id', 
                          axis=1))

            indices_frame = indices.to_frame()
            sub_df = pd.concat([sub_df, indices_frame], axis=1)
            stag_distance.append(sub_df)
            
        stag_distance.append(sub_df)
                
    return stag_distance



f = {'distance':['min', 'max', 'mean']}

def deduplicate_distance_matrix_afval(stag_distance, df1, buffer, bovengrens):
    
    """
    steps to munge the raw distance matrix into a clean deduplicated version
    args:
        stag_distance: frame resulting from create_distance_matrix_afval function
        df1 : same dataframe one containing geometry column (points) as fed to the create_distance_matrix_afval function
        bovengrens: value to fill all the distances not confom the norm for the particular fractie
    """
    
    stag_dm = pd.concat(stag_distance, axis=0, sort=False)
    stag_dm.distance = stag_dm['distance'].astype(float)
    logger.info('Raw dm has shape:{}'.format(stag_dm.shape))
    
    
    dist_agg = stag_dm.groupby(['cluster_toewijzing']).agg(f).reset_index()
    dist_agg.columns = [f'{i}_{j}' if j != '' else f'{i}' for i,j in dist_agg.columns]
    
    
    
    keep_cols = ['landelijk_pnd_id','pnd_geom','geometry', 'cluster_toewijzing']
    dist_agg = pd.merge(df1[keep_cols], dist_agg, on = ['cluster_toewijzing'], 
                        how='left', indicator=True)
    logger.info('Aggregated dm has shape:{}'.format(dist_agg.shape))
    logger.info('merge results:\n{}'.format(dist_agg._merge.value_counts()))
    
    
    dist_agg['distance_min'] = (dist_agg['distance_min'].multiply(1000)
                                 .astype(float)
                                 .fillna(bovengrens)
                                 .map('{:.0f}'.format))
    logger.info('Filled values above the {} norm with bovengrens value: {}'\
               .format(buffer, bovengrens))
    
    dist_agg['distance_min'] = dist_agg['distance_min'].astype(int)
    # we want to plot on pnd_geom, Geopandas only accepts 'geometry' for plotting, so:
    dist_agg = dist_agg.rename(columns = {'geometry': 'geom_point', 'pnd_geom': 'geometry'})
    
    pnd_mean_dist = dist_agg.groupby(['landelijk_pnd_id'])['distance_min'].mean().to_frame().reset_index()
    pnd_mean_dist = pnd_mean_dist.rename(columns={'distance_min': 'pnd_dist_mean'})
    pnd_mean_dist.pnd_dist_mean = pnd_mean_dist.pnd_dist_mean.map('{:.0f}'.format)
    
    final = pd.merge(dist_agg, pnd_mean_dist, on=['landelijk_pnd_id'], how='left')
    logger.info('Final dm has shape:{}'.format(final.shape))
    logger.info('columns dm {}'.format(final.columns.tolist()))
    
    num_cols = ['distance_min', 'distance_max', 'distance_mean']
    fig, ax = plt.subplots(len(num_cols), 1, figsize= [9,6])
    
    print ('histogram numerical distance columns: ')
    for i, col in enumerate(final[num_cols].columns):
        final[final.distance_min < bovengrens][col].dropna().hist(bins=40, ax=ax[i])
        ax[i].set_title(col)
        plt.tight_layout()
    
    return final

# ========================================
# general distance_matrix_ function
def create_distance_matrix_general(df1, df2, buffer = int, 
                                 include_nearest_point = None, n=int):
    """
    calculate distance matrix frames. See ../helper_functions/distance_matrix.py 
    Make sure the you feed geoPandas df with a geometry column
    args:
        df1 : dataframe one containing geometry column (points)
        df2 : dataframe 2 containing geometry column (points)
        buffer: buffer in meters around the geometry column in df2 
        include_nearest_point: Find nearest point,return corresponding value 
        from specified column.Caution very slow on big sets.
        n : number of iterations. set n=len(df2) to loop through full set
    """
    n=n  
    logger.info('Building dm (buffer: {} with {} iterations'.format(buffer, n))
    
    stag_distance = []
    
    for i, row in enumerate(tqdm_notebook(list(df2['buffer'][:n]))):
        sub_df = df1.loc[(df1.geometry.within(df2['buffer'][i])), :]
        sub_df = (sub_df.apply(calculate_distance, 
                               dest_geom= df2['geometry'][i], 
                     target_col= 'distance', axis=1))
        print ('Shape sub_df {} = {}'.format(i, sub_df.shape))
        
        if include_nearest_point:
            indices = (sub_df.apply(find_nearest_point, 
                          geom_union=df2.unary_union, 
                          df1=sub_df, 
                          df2=df2, 
                          geom1_col='geometry', 
                          src_column='container_id', 
                          axis=1))

            indices_frame = indices.to_frame()
            sub_df = pd.concat([sub_df, indices_frame], axis=1)
            stag_distance.append(sub_df)
            
        stag_distance.append(sub_df)
                
    return stag_distance