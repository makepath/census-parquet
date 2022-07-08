"""
Generate Synthetic People

We create a table with a single point for each person within census.
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import dask_geopandas
from dask.dataframe.dispatch import make_meta_dispatch
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
gpd.options.use_pygeos=True


def polygons_to_points(gdf):
    keep_points = []
    rng = np.random.default_rng()
    if gdf.empty:
        return pd.DataFrame(
            {"x": [float(0.0)],
             "y": [float(0.0)],
             "GEOID": ["00"], 
             "R":[" "]
            }
        ).set_index("GEOID")
    for index, row in gdf.iterrows():
        pop = int(row["POP"])
        if pop==0:
            continue
        _w = row["P0010003"]
        _b = row["P0010004"]
        _n = row["P0010005"]
        _a = row["P0010006"]
        _hpi = row["P0010007"]
        _o = row["P0010008"]
        _m = row["P0010009"]
        len_within = 0
        it = 1
        while len_within < pop:
            x_min, y_min, x_max, y_max = row["geometry"].bounds
            xs = np.random.uniform(x_min, x_max, pop * it)
            ys = np.random.uniform(y_min, y_max, pop * it)
            gdf_points = gpd.GeoSeries(gpd.points_from_xy(xs, ys), crs=3857)
            within_points = gdf_points.clip(row["geometry"])
            len_within = len(within_points)
            it+=1
        within_pdf = pd.DataFrame(
            {
                "x": within_points.iloc[:pop].x,
                "y": within_points.iloc[:pop].y,
                "GEOID": [index] * pop,
                "R": ["w"] * _w + ["b"] * _b + ["n"] * _n + ["a"] * _a + ["hpi"] * _hpi + ["o"] * _o + ["m"] * _m                    
            }
        )
        keep_points.append(within_pdf)
    return pd.concat(keep_points, ignore_index=True).set_index("GEOID")


def main():
    blocks_ddf = dask_geopandas.read_parquet("outputs/census_blocks_pops.parquet", calculate_divisions=True)
    meta_df = pd.DataFrame(
    			{"x": [float(0.0)],
     			 "y": [float(0.0)],
     			 "GEOID": ["00"],
    			 "R":[" "]}
		).set_index("GEOID")
    print("Generating Synthetic People")
    with ProgressBar():
        blocks_ddf.map_partitions(polygons_to_points,
				  meta=meta_df,
				).to_parquet("outputs/synthetic_people.parquet", write_metadata_file=True)


if __name__ == "__main__":
    main()
