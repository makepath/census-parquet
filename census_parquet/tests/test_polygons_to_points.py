import pytest
import numpy as np
import geopandas as gpd
import pandas as pd
import dask_geopandas
from pygeos import convex_hull, Geometry, to_shapely
from census_parquet.generate_synthetic_people import polygons_to_points
from census_parquet.tests.general_checks import (
    general_output_checks_dask,
    general_output_checks_pd,
)

def create_test_gdf():
    gdf = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres")).to_crs(3857).drop(columns=["pop_est", "continent", "name", "iso_a3", "gdp_md_est"])
    gdf = gdf.iloc[[20, 23]]
    gdf["GEOID"] = gdf.index.values
    gdf["POP"] = 100*np.ones((2,),dtype=int)
    gdf["P0010003"] = 10*np.ones((2,),dtype=int)
    gdf["P0010004"] = 10*np.ones((2,),dtype=int)
    gdf["P0010005"] = 10*np.ones((2,),dtype=int)
    gdf["P0010006"] = 10*np.ones((2,),dtype=int)
    gdf["P0010007"] = 30*np.ones((2,),dtype=int)
    gdf["P0010008"] = 10*np.ones((2,),dtype=int)
    gdf["P0010009"] = 20*np.ones((2,),dtype=int)
    return gdf

def test_polygons_to_points_gpd():
    gdf = create_test_gdf()
    out = polygons_to_points(gdf)
    general_output_checks_pd(gdf, out)


