working_dir = '/Users/natalieodell/Documents/census_data/'

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
    print(df.memory_usage())
    npartitions = int(((df.memory_usage().sum() / (1024 * 1024))//128)+1)
    ddf = dask_geopandas.from_geopandas(df, npartitions=npartitions) 
    outputname = create_outputname(filepath)
    ddf.to_parquet(outputname)
    print(f'Finished {outputname}, {npartitions} partitions')
    return ddf

import glob
for file in glob.glob(#'directory with census files*.zip"):
    filepath = file
    load_and_partition(filepath)
