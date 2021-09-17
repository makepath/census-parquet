import warnings
from pathlib import Path

import dask
import geopandas
import pandas as pd


warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

DTYPES = {
    "AFFGEOID": "string",
    "AFFGEOID20": "string",
    "AIANNHCE": "int",
    "AIANNHNS": "int",
    "ALAND": "int",
    "ALAND20": "int",
    "AWATER": "int",
    "AWATER20": "int",
    "ANRCFP": "int",
    "ANRCNS": "int",
    "BLKGRPCE": "category",
    "CBSAFP": "int",
    "CD116FP": "int",
    "CDSESSN": "int",
    "CNECTAFP": "category",
    "CONCTYFP": "int",
    "CONCTYNS": "int",
    "COUNTYNS": "string",
    "COUNTYFP": "category",
    "COUNTYFP20": "category",
    "COUSUBFP": "category",
    "COUSUBNS": "string",
    # "CSAFP": pd.Int64Dtype(),  # can't astype object -> Int64
    "DIVISIONCE": "int",
    "ELSDLEA": "int",
    "GEOID": "string",
    "GEOID20": "string",
    "LSAD": "category",
    "LSAD20": "category",
    "LSY": "category",
    "METDIVFP": "int",
    "NAME": "string",
    "NAME20": "string",
    "NAMELSAD": "string",
    "NAMELSAD20": "string",
    "NAMELSADCO": "category",
    "NCTADVFP": "int",
    "NECTAFP": "int",
    "PARTFLG": "category",
    "PLACEFP": "int",
    "PLACENS": "int",
    "REGIONCE": "int",
    "SCSDLEA": "int",
    "SLDLST": "string",
    "SLDUST": "string",
    "STATE_NAME": "category",
    "STATEFP": "category",
    "STATEFP20": "category",
    "STATENS": "int",
    "STUSPS": "category",
    "SUBMCDFP": "int",
    "SUBMCDNS": "int",
    "TBLKGPCE": "category",
    "TRACTCE": "int",
    "TTRACTCE": "category",
    "TRSUBCE": "int",
    "TRSUBNS": "int",
    "UNSDLEA": "int",
    "VTDI20": "category",
    "VTDST20": "string",
}


def process_boundary_file(path: Path) -> Path:
    print(f"Started {path}")
    gdf = geopandas.read_file(path, driver="SHP")
    gdf = gdf.astype({k: DTYPES[k] for k in set(gdf.columns) & set(DTYPES)})
    if "CSAFP" in gdf.columns:
        gdf["CSAFP"] = gdf["CSAFP"].astype("float64").astype(pd.Int64Dtype())

    output = Path(path).parent / "boundary_outputs" / path.with_suffix(".parquet").name
    output.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output, index=False)
    print(f"Finished {output}")
    return output


def main():
    files = list(Path("census_boundaries").glob("*.zip"))
    print(f"Found {len(files)} files")
    results = [dask.delayed(process_boundary_file)(file) for file in files]
    dask.compute(results)


if __name__ == "__main__":
    main()
