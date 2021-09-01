#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 25 17:02:43 2021

@author: natalieodell
"""
#Merges Census Blocks with Population Data

import pandas as pd

state_1 = "wy000012020.pl"
state_geo = "wygeo2020.pl" 

import dask_geopandas

from os import path

filepath = '/Users/natalieodell/Documents/popblocks.nosync/FULL_tlgdb_2020_a_us_block.gdb.parq/WY'

def create_outputname(filepath):
    layername = path.splitext(path.split(filepath)[-1])[0]
    outputname = layername + ".parq"
    return outputname

seg_1_header_df = pd.read_excel(
    "2020_PLSummaryFile_FieldNames.xlsx",
    sheet_name="2020 P.L. Segment 1 Fields"
)
geo_header_df = pd.read_excel(
    "2020_PLSummaryFile_FieldNames.xlsx",
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

df = pd.merge(
    left=geo_df[["LOGRECNO", "GEOID", "STUSAB", "BLOCK", "SUMLEV"]],
    right=seg_1_df[["LOGRECNO", "P0010001"]],
    how="left",
    on="LOGRECNO"
)

block_df = df[df.SUMLEV == 750]

block_df['GEOID'] = block_df['GEOID'].str.replace("7500000US", "")

ddf = dask_geopandas.read_parquet(filepath)

ddf.crs = 4326
ddf = ddf.to_crs(epsg=3857)
ddf = ddf.compute()


merge_df = pd.merge(
    left=ddf,
    right=block_df,
    how="left",
    on="GEOID"
)

print(f'Processing {merge_df}')
df = merge_df
df['COUNTY'] = df['GEOID'].str[:5]
numpart = df['COUNTY'].nunique()
print(df.memory_usage())
npartitions = int(((df.memory_usage().sum() / (1024 * 1024))//128)+1)
ddf = dask_geopandas.from_geopandas(df, npartitions=npartitions) 
outputname = create_outputname(filepath)
ddf.to_parquet(outputname,partition_on=['COUNTY'])
print(f'Finished {outputname}, {numpart} partitions')
