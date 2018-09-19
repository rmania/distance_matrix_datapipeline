%matplotlib inline
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator
from mpl_toolkits.axes_grid1 import make_axes_locatable

import pandas as pd
pd.set_option('display.max_columns', 100)
import numpy as np
import geopandas as gpd
from shapely import wkt, wkb
from shapely.geometry import Polygon, Point, LinearRing, MultiPoint, shape

import contextily as ctx



def add_basemap(ax, zoom, url='httpS://t1.data.amsterdam.nl/topo_wm_zw/tileZ/tileX/tileY.png',
               alpha=.5):
    xmin, xmax, ymin, ymax = ax.axis()
    basemap, extent = ctx.bounds2img(xmin, ymin, xmax, ymax, zoom=zoom, url=url)
    ax.imshow(basemap, extent=extent, interpolation='bilinear', alpha=alpha)
    # restore original x/y limits
    ax.axis((xmin, xmax, ymin, ymax))


# https://matplotlib.org/2.2.2/_modules/matplotlib/colorbar.html
def update_ticks(cb, cax):
        """
        Force the update ticks and ticklabels of colorbar. This must be
        called whenever the tick locator and/or tick formatter changes.
        """
        #cax = cb.ax
        ticks, ticklabels, offset_string = cb._ticker()
        if cb.orientation == 'vertical':
            cax.yaxis.set_ticks(levels)
            cax.set_yticklabels(labels)
            cax.yaxis.get_major_formatter().set_offset_string(offset_string)

        else:
            cax.xaxis.set_ticks(levels)
            cax.set_xticklabels(labels)
            cax.xaxis.get_major_formatter().set_offset_string(offset_string)
            

def fixed_aspect_ratio(ratio):
    '''
    Set a fixed aspect ratio on matplotlib plots 
    regardless of axis units
    '''
    xvals,yvals = gca().axes.get_xlim(),ax.get_ylim() #gca().axes

    xrange = xvals[1]-xvals[0]
    yrange = yvals[1]-yvals[0]
    ax.set_aspect(ratio*(xrange/yrange), adjustable='box') # gca()
    
    