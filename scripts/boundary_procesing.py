#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 08:48:09 2021

@author: natalieodell
"""
import dask.bag as bag

import geopandas as gpd

import dask_geopandas

from os import path
import glob

def load_dtype(filename):
    print(f'Started {filename}')
    gdf = gpd.read_file(filename, driver='SHP')
    
    #dtype conversions
    try:
        gdf['AFFGEOID'] = gdf['AFFGEOID'].astype('str', errors = 'ignore')
    except KeyError:
        pass
    try:    
        gdf['AFFGEOID20'] = gdf['AFFGEOID20'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['AIANNHCE'] = gdf['AIANNHCE'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['AIANNHNS'] = gdf['AIANNHNS'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['ALAND'] = gdf['ALAND'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['ALAND20'] = gdf['ALAND20'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['AWATER'] = gdf['AWATER'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['AWATER20'] = gdf['AWATER20'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['ANRCFP'] = gdf['ANRCFP'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['ANRCNS'] = gdf['ANRCNS'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['BLKGRPCE'] = gdf['BLKGRPCE'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['CBSAFP'] = gdf['CBSAFP'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['CD116FP'] = gdf['CD116FP'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['CNECTAFP'] = gdf['CNECTAFP'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['CONCTYFP'] = gdf['CONCTYFP'].astype('int')
    except KeyError:
        pass
    try:
        gdf['CONCTYNS'] = gdf['CONCTYNS'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['COUNTYFP'] = gdf['COUNTYFP'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['COUNTYFP20'] = gdf['COUNTYFP20'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['COUSUBFP'] = gdf['COUSUBFP'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['COUSUBNS'] = gdf['COUSUBNS'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['CSAFP'] = gdf['CSAFP'].astype(float,errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['DIVISIONCE'] = gdf['DIVISIONCE'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['ELSDLEA'] = gdf['ELSDLEA'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['GEOID'] = gdf['GEOID'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['GEOID20'] = gdf['GEOID20'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['LSAD'] = gdf['LSAD'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['LSAD20'] = gdf['LSAD20'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['LSY'] = gdf['LSY'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['METDIVFP'] = gdf['METDIVFP'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['NAME'] = gdf['NAME'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['NAME20'] = gdf['NAME20'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['NAMELSAD'] = gdf['NAMELSAD'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['NAMELSAD20'] = gdf['NAMELSAD20'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['NAMELSADCO'] = gdf['NAMELSADCO'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['NCTADVFP'] = gdf['NCTADVFP'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['NECTAFP'] = gdf['NECTAFP'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['PARTFLG'] = gdf['PARTFLG'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['PLACEFP'] = gdf['PLACEFP'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['PLACENS'] = gdf['PLACENS'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:    
        gdf['REGIONCE'] = gdf['REGIONCE'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['SCSDLEA'] = gdf['SCSDLEA'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['SLDLST'] = gdf['SLDLST'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['SLDUST'] = gdf['SLDUST'].astype('str',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['STATE_NAME'] = gdf['STATE_NAME'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['STATEFP'] = gdf['STATEFP'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['STATEFP20'] = gdf['STATEFP20'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['STATENS'] = gdf['STATENS'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['STUSPS'] = gdf['STUSPS'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['SUBMCDFP'] = gdf['SUBMCDFP'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['SUBMCDNS'] = gdf['SUBMCDNS'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['TBLKGPCE'] = gdf['TBLKGPCE'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['TRACTCE'] = gdf['TRACTCE'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['TTRACTCE'] = gdf['TTRACTCE'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['TRSUBCE'] = gdf['TRSUBCE'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['TRSUBNS'] = gdf['TRSUBNS'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['UNSDLEA'] = gdf['UNSDLEA'].astype('int',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['VTDI20'] = gdf['VTDI20'].astype('category',errors ='ignore')
    except KeyError:
        pass
    try:
        gdf['VTDST20'] = gdf['VTDST20'].astype('str',errors ='ignore')
    except KeyError:
        pass
    
    #write to parquet
    
    outputname = path.join(path.dirname(filename), 'boundary_outputs', path.splitext(path.split(filename)[-1])[0]+ '.parquet') 
    ddf = dask_geopandas.from_geopandas(gdf, npartitions=1)
    ddf.to_parquet(outputname)
    print(f'Finished {outputname}')
    return filename
   
if __name__ == '__main__':
    files = glob.glob('./cb_2020_us_all_500k/*.zip')
    print(f'Found {len(files)} files')
    bg = bag.from_sequence(files).map(load_dtype)
    bg.compute()
    

