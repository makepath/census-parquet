import pytest
import geopandas as gpd
import dask_geopandas
import pandas as pd

def general_output_checks_pd(input_gdf: gpd.GeoDataFrame, output_df: pd.DataFrame):
    assert isinstance(output_df, pd.DataFrame)

def general_output_checks_dask(input_gdf: dask_geopandas.GeoDataFrame, output: str):
    a = dask_geopandas.read_parquet(output)
    assert a.known_divisions
    
