#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 15:13:52 2021

@author: natalieodell
"""

import numpy as np
import dask_geopandas
ddf = dask_geopandas.read_parquet(
    "/Users/natalieodell/Documents/makepath.nosync/census_data/2020/tlgdb_2020_a_us_block.gdb.parquet/tlgdb_2020_a_us_block.gdb.parq/AK.parq")
ddf = ddf.replace(np.nan,0)

ddf['GEOID'] = ddf['GEOID'].astype('int').compute()
del ddf['SUFFIX']
ddf['ALAND'] = ddf['ALAND'].astype('int').compute()
ddf['AWATER'] = ddf['AWATER'].astype('int').compute()
ddf['INTPTLAT'] = ddf['INTPTLAT'].str.replace('+','').astype(float).compute()
ddf['INTPTLON'] = ddf['INTPTLON'].str.replace('-','').astype(float).compute()
del ddf['LOGRECNO']
ddf['BLOCK'] = ddf['BLOCK'].astype('int')
del ddf['STUSAB']
del ddf['SUMLEV']
ddf['P0010001'] = ddf['P0010001'].astype('int').compute()

ddf = ddf.repartition(npartitions=1)
ddf.to_parquet("AK.parquet", partition_on = ['COUNTY'])