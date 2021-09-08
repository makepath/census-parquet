#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  8 10:35:55 2021

@author: natalieodell
"""

import dask.bag as bag
import glob
import pandas as pd
import geopandas as gpd
import dask_geopandas
from os import path

statelookup = { '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI', '56': 'WY', '72': 'PR' }

def load(filename):
    print(f'Started {filename}')

    census_geom = gpd.read_file(filename, driver='SHP')

    # add population data
    FIPS = path.split(filename)[-1].split('_')[2]
    ABBR = statelookup.get(FIPS)
    
    if not ABBR:
        print(f'unable to parse FIPS:{FIPS}')
        return filename
    ABBR = ABBR.lower()
    state_1 = f"/Users/natalieodell/Documents/makepath.nosync/census_data/popblocks/{ABBR}000012020.pl"
    state_geo = f"/Users/natalieodell/Documents/makepath.nosync/census_data/popblocks/{ABBR}geo2020.pl" 
    
    if not path.exists(state_1):
        print(f' unable to find {state_1}')
        gdf = census_geom
        gdf['GEOID20'] = gdf['GEOID20'].astype('str')
        gdf['COUNTY20'] = gdf['GEOID20'].str[:5]
        outputname = path.join(path.dirname(filename), 'outputs', path.split(filename)[-1]+'.parquet') 
        ddf = dask_geopandas.from_geopandas(gdf, npartitions=1)
        ddf.to_parquet(outputname) #partition_on=['COUNTY20'])
        print(f'Finished {outputname}')
        return filename
    if not path.exists(state_geo):
        print(f' unable to find {state_geo}')
        return filename
    
    seg_1_header_df = pd.read_excel(
    "/Users/natalieodell/Documents/makepath.nosync/census_data/2020_PLSummaryFile_FieldNames.xlsx",
    sheet_name="2020 P.L. Segment 1 Fields"
    )
    geo_header_df = pd.read_excel(
        "/Users/natalieodell/Documents/makepath.nosync/census_data/2020_PLSummaryFile_FieldNames.xlsx",
        sheet_name="2020 P.L. Geoheader Fields"
        )

    seg_1_df = pd.read_csv(
        state_1,
        encoding='latin-1',
        delimiter="|",
        names=seg_1_header_df.columns.to_list(),
        low_memory=False
        )
    geo_df = pd.read_csv(
        state_geo,
        encoding='latin-1',
        delimiter="|",
        names=geo_header_df.columns.to_list(),
        low_memory=False
        )

    pop_df = pd.merge(
        left=geo_df[["LOGRECNO", "GEOID20", "STUSAB", "BLOCK", "SUMLEV"]],
        right=seg_1_df[["LOGRECNO", "P0010001"]],
        how="left",
        on="LOGRECNO"
    )
    
    block_df = pop_df[pop_df.SUMLEV == 750]
    
    block_df['GEOID20'] = block_df['GEOID20'].str.replace("7500000US", "")
    
    gdf = pd.merge(
        left=census_geom,
        right=block_df,
        how="left",
        left_on="GEOID20",
        right_on="GEOID20"
    )
    #set dtypes
    gdf['STATEFP20'] = gdf['STATEFP20'].astype('category')
    gdf['COUNTYFP20'] = gdf['COUNTYFP20'].astype('category')
    gdf['TRACTCE20'] = gdf['TRACTCE20'].astype('int')
    gdf['BLOCKCE20'] = gdf['BLOCKCE20'].astype('int')
    del gdf['MTFCC20']
    del gdf['UR20']
    del gdf['UACE20']
    del gdf['UATYPE20']
    del gdf['FUNCSTAT20']
    gdf['INTPTLON20'] = gdf['INTPTLON20'].astype(float)
    gdf['INTPTLAT20'] = gdf['INTPTLAT20'].str.replace('+','').astype(float)
    del gdf['LOGRECNO']
    gdf['STUSAB'] = gdf['STUSAB'].astype('category')
    del gdf['BLOCK']
    del gdf['SUMLEV']
    gdf['P0010001'] = gdf['P0010001'].astype(float)
    # set partitions by county
    gdf['GEOID20'] = gdf['GEOID20'].astype('str')
    gdf['COUNTY20'] = gdf['GEOID20'].str[:5]
    # change to dask_geopandas.GeoDataFrame
    # write to parquet
    outputname = path.join(path.dirname(filename), 'outputs', path.split(filename)[-1]+'.parquet') 
    ddf = dask_geopandas.from_geopandas(gdf, npartitions=1)
    ddf.to_parquet(outputname) #partition_on=['COUNTY20'])
    print(f'Finished {outputname}')
    return filename

import os
import pyarrow.parquet as pq

def combine_parquet_files(input_folder, target_path):
    try:
        files = []
        for file_name in os.listdir(input_folder):
            files.append(pq.read_table(os.path.join(input_folder, file_name)))
        #with pq.ParquetWriter(target_path,
         #       files[0].schema,
          #      version='2.0',
           #     compression='gzip',
            #    use_dictionary=True,
             #   data_page_size=2097152,
              #  write_statistics=True) as writer:
        for f in files:
            pq.write_to_dataset(
                f,
                root_path=target_path,
                partition_cols=['COUNTY20'])
    except Exception as e:
        print(e)


if __name__ == '__main__': 

    files = glob.glob('/Users/natalieodell/Documents/DC_RI/*.zip')
    bg = bag.from_sequence(files).map(load)
    bg.compute()
    #load(files)
    combine_parquet_files('outputs', 'combinedtest.parquet')
