'''
version:  0.1 JFA

WORK:
    -- eventually develop list of geoschema via directory inspection
    -- use WKT for CRS instead of Proj4
    -- normalizing the geometry field name to WKT should not be hard coded

Build a geopackage with desired layers
'''

from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely import wkt
import json
import datetime as dt


def getdataframe(maindir, geoschema):
    
    gooddf = True

    # get the list of columns we want for this layer
    schemadata = json.load(open(geoschema))
    layername = schemadata["layername"]
    print('\n\t\tGetting list of columns for a layer called :' + layername)  
    geodata = maindir / schemadata["geocsv"]
    print('\n\t\tUsing data from csv file: ' + geodata.stem)

    
    # get the field dictionaries
    l_fielddicts = schemadata["fields"]
    
    # build the list of columns
    l_columns = []
    d_columndict = {}
    for fielddict in l_fielddicts:
        d_columndict = {k:v for (k,v) in fielddict.items() if k =='name'}
        for k,v in d_columndict.items():
            l_columns.append(d_columndict[k])
            
    try:
        df = pd.read_csv(geodata, usecols = l_columns)
        
        # normalize the WKT column name
        if 'opening' in df.columns:
            df = df.rename(columns={'opening': 'wkt'})
        else:
            pass
        
        # drop rows with no wkt value
        df_no = df[df['wkt'].isna()]
        print('\n\t\t' + str(len(df_no.index)) + ' rows dropped as have no WKT value')
        indexNames = df[df['wkt'].isna()].index
        df.drop(indexNames , inplace=True)

    except:
        gooddf = False
    
    return(df, layername, gooddf)
    
def savelayer(df, geopackpath, layername):
    
    goodpack = True

    # turn the wkt value into a geometry
    # could do this all in one go with
    #    df['wkt'] = df['wkt'].apply(wkt.loads)
    # but need to find rows that are bad so iterate with try
    
    for idx in df.index:
        try:
            oldwkt = df.at[idx, 'wkt']
            newwkt = wkt.loads(oldwkt)
            df.at[idx, 'wkt'] = newwkt
    
        except Exception as error:
            print(str(error))
            print(df.at[idx, 'Source'])
            print(df.at[idx, 'Identifier'])
            input(df.at[idx, 'wkt'])

    
    # build geodataframe
    gdf = gpd.GeoDataFrame(df, geometry='wkt')
    
    # adjust geometry values to account for Keros local grid
    gdf = gdf.translate(xoff=0.0, yoff=0.0, zoff=0.0)

    
    # set projection
    s_crs = '''+proj=tmerc +lat_0=36.8924527603922 +lon_0=25.6468737208999 
    +k=1 +x_0=4000 +y_0=4000 +datum=WGS84 +units=m +no_defs'''

    gdf.crs = s_crs
    
    # save gdf as layer in geopackage
    try:
        gdf.to_file(geopackpath, layer=layername, driver="GPKG")
        print('\n\t\tBuilt a GIS layer named: ' + layername)
    except:
        goodpack = False
        
    return(goodpack)
    
    
def buildgeopackage(maindir):
    
    goodpack = True
    
    # get a name for the new geopackage
    # get a timestamp for geopackage name
    o_dt = dt.datetime.utcnow()
    s_timestamp = str(o_dt.isoformat(sep='+', timespec='minutes'))
    s_geopack = 'data/keros-' + s_timestamp + '.gpkg'
    geopackpath = maindir / s_geopack
    print('\n\t\tWill build a geopackage named ' + geopackpath.stem) 
    
    # get the list of geoschemas - eventually this should be a call to
    # directory for the full list
    l_geoschema = ['geocontextnoasuphasepottery.json','geocontextnoasusfsummary.json']
    
    # iterate through schemas and build layers in geopackage
    for item in l_geoschema:
        s_path = 'schema/geoschema/' + item
        geoschema = maindir / s_path

        # get df and build layer
        df, layername, gooddf = getdataframe(maindir, geoschema)
        if gooddf:
            goodpack = savelayer(df, geopackpath, layername)
        else:
            pass
    
    return(goodpack)
    
    

if __name__ == "__main__":
    maindir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase')
    goodpack = buildgeopackage(maindir)
    print(goodpack)
