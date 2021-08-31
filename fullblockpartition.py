#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 10 14:50:44 2021

@author: natalieodell
"""

'''
Script parses for zip files in a directory and partitions and converts to 
.parq file
'''
working_dir = '/Users/natalieodell/Documents/makepath.nosync/census_data/census20block'

import geopandas as gpd

import dask_geopandas

from os import path

def create_outputname(filepath):
    layername = path.splitext(path.split(filepath)[-1])[0]
    outputname = layername + ".parq"
    return outputname

def load_and_partition(filepath):
    print(f'Processing {filepath}')
    df = gpd.read_file(filepath)
    df['COUNTY'] = df['GEOID'].str[:5]
    numpart = df['COUNTY'].nunique()
    print(df.memory_usage())
    npartitions = int(((df.memory_usage().sum() / (1024 * 1024))//128)+1)
    ddf = dask_geopandas.from_geopandas(df, npartitions=npartitions) 
    outputname = create_outputname(filepath)
    ddf.to_parquet(outputname,partition_on=['COUNTY'])
    print(f'Finished {outputname}, {numpart} partitions')
    return ddf

import glob
for file in glob.glob("/Users/natalieodell/Documents/makepath.nosync/census_data/census20block/*.zip"):
    filepath = file
    load_and_partition(filepath)