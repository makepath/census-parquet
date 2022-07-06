"""
Processing Census Blocks.

We create two logical tables:

1. Geometries only
2. Populations only

This is driven by the Census Bureau not providing population statistics
for territories (yet?).
"""
from pathlib import Path
import warnings

import dask
import dask.dataframe as dd
import dask_geopandas
from dask.diagnostics import ProgressBar
import geopandas
import pandas as pd

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")


statelookup = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
    "72": "PR",
}
SUMMARY_TABLE = "./population_stats/2020_PLSummaryFile_FieldNames.xlsx"


def process_pop(file):
    FIPS = file.stem.split("_")[2]
    ABBR = statelookup[FIPS]

    root = Path("population_stats")
    state_1 = root / (ABBR.lower() + "000012020.pl")
    state_geo = root / (ABBR.lower() + "geo2020.pl")

    seg_1_header_df = pd.read_excel(
        SUMMARY_TABLE, sheet_name="2020 P.L. Segment 1 Fields"
    )

    geo_header_df = pd.read_excel(
        SUMMARY_TABLE, sheet_name="2020 P.L. Geoheader Fields"
    )

    seg_1_df = pd.read_csv(
        state_1,
        encoding="latin-1",
        delimiter="|",
        names=seg_1_header_df.columns.to_list(),
        low_memory=False,
    ).drop(columns=["STUSAB"])

    geo_df = pd.read_csv(
        state_geo,
        encoding="latin-1",
        delimiter="|",
        names=geo_header_df.columns.to_list(),
        low_memory=False,
    )
    geo_df = geo_df[geo_df["SUMLEV"] == 750]

    block_df = pd.merge(
        left=geo_df[["LOGRECNO", "GEOID", "STUSAB"]],
        right=seg_1_df,
        how="left",
        on="LOGRECNO",
    ).drop(columns=["LOGRECNO", "CHARITER", "STUSAB", "FILEID", "CIFSN"])
    block_df["GEOID"] = block_df["GEOID"].str.replace("7500000US", "")
    block_df = block_df.set_index("GEOID").sort_index()

    assert block_df.index.is_unique
    return block_df   


def process_geo(file):
    dtypes = {
        "STATEFP": "int",
        "COUNTYFP": "int",
        "TRACTCE": "int",
        "BLOCKCE": "int",
        "HOUSING": "int",
        "POP":    "int"
    }

    gdf = (
        geopandas.read_file(file, driver="SHP")
        .drop(columns=["MTFCC20", "UR20", "UACE20", "UATYPE20", "FUNCSTAT20", "NAME20"])
        .rename(columns=lambda x: x.rstrip("20"))
        .astype(dtypes)
        .set_index("GEOID")
    )
    gdf["INTPTLON"] = pd.to_numeric(gdf["INTPTLON"])
    gdf["INTPTLAT"] = pd.to_numeric(gdf["INTPTLAT"])
    gdf = gdf.replace([None],0)

    return gdf


def process(file):
    geo = process_geo(file)
    FIPS = file.stem.split("_")[2]

    if FIPS in statelookup:
        pop = process_pop(file)
        result = pd.merge(geo, pop)
        assert len(result) == len(geo)
    else:
        pop = None

    return file, geo, pop

def process_pop_geo(file):
    geo = process_geo(file)
    FIPS = file.stem.split("_")[2]
    block_ddf = dask_geopandas.from_geopandas(geo, npartitions=1)
    output_geo = Path(f"tmp/geo/{file.stem.split('_')[2]}.parquet")
    output_geo.parent.mkdir(parents=True, exist_ok=True)
    block_ddf.to_parquet(output_geo) 
    if FIPS in statelookup:
        pop = process_pop(file)
        pop_ddf = dd.from_pandas(pop, npartitions=1)
        output_pop = Path(f"tmp/pop/{FIPS}.parquet")
        output_pop.parent.mkdir(parents=True, exist_ok=True)
        pop_ddf.to_parquet(output_pop)
        result = pd.merge(geo,pop,left_index=True,right_index=True)
        result = result[['POP',
			 'P0010003',
                         'P0010004',
                         'P0010005',
                         'P0010006',
                         'P0010007',
                         'P0010008',
                         'P0010009',
                         'geometry']
                        ]
        result = result.to_crs(3857)
        result = dask_geopandas.from_geopandas(result,npartitions=1)
        assert len(result) == len(geo)
        output = Path(f"tmp/comb/{file.stem.split('_')[2]}.parquet")
        output.parent.mkdir(parents=True,exist_ok=True)
        result.to_parquet(output)
    else:
        pop = None
    return output, output_pop, output_geo


def main():
    files = list(Path("TABBLOCK20").glob("*.zip"))
    
    combs = [dask.delayed(process_pop_geo)(file)
            for file in files
            if file.stem.split("_")[2] in statelookup
    ]

    print("combining geo and pops")
    with ProgressBar():
        outs  = dask.compute(*combs)
    
    comb_files = [x[0] for x in outs]
    pop_files = [x[1] for x in outs]
    geo_files = [x[2] for x in outs]
    pop = dd.concat([dd.read_parquet(f) for f in sorted(pop_files)])
    geo = dd.concat([dask_geopandas.read_parquet(f) for f in sorted(geo_files)])
    assert pop.known_divisions
    assert geo.known_divisions
    
    comb = dd.concat([dask_geopandas.read_parquet(f) for f in sorted(comb_files)])
    assert comb.known_divisions

    Path("outputs").mkdir(exist_ok=True)
    print("spatial partitioning combined files")
    with ProgressBar():
        comb.calculate_spatial_partitions()

    print("finalizing census blocks and population data")
    with ProgressBar():
        comb.to_parquet("outputs/census_blocks_pops.parquet", write_metadata_file=True)

    print("finalizing population files")
    with ProgressBar():
        pop.to_parquet("outputs/census_population.parquet", write_metadata_file=True)

    print("computing spatial partitions for geo files")
    with ProgressBar():
        geo.calculate_spatial_partitions()

    print("finalizing geo files")
    with ProgressBar():
        geo.to_parquet("outputs/census_blocks_geo.parquet", write_metadata_file=True)
    
    print("validating")
    a = dd.read_parquet("outputs/census_population.parquet")
    assert a.known_divisions

    b = dask_geopandas.read_parquet("outputs/census_blocks_geo.parquet")
    assert b.known_divisions
    
    c = dask_geopandas.read_parquet("outputs/census_blocks_pops.parquet")
    assert c.known_divisions
    
    print("complete")

if __name__ == "__main__":
    main()
