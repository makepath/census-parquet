import dask.bag as bag
import dask_geopandas
import geopandas as gpd
import glob
import os


def load_dtype(filename):
    print(f'Started {filename}')
    gdf = gpd.read_file(filename, driver='SHP')

    #dtype conversions
    mapping = {
        "AFFGEOID": "str",
        "AFFGEOID20": "str",
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
        "CNECTAFP": "category",
        "CONCTYFP": "int",
        "CONCTYNS": "int",
        "COUNTYFP": "category",
        "COUNTYFP20": "category",
        "COUSUBFP": "category",
        "COUSUBNS": "str",
        "CSAFP": "float",
        "DIVISIONCE": "int",
        "ELSDLEA": "int",
        "GEOID": "str",
        "GEOID20": "str",
        "LSAD": "category",
        "LSAD20": "category",
        "LSY": "category",
        "METDIVFP": "int",
        "NAME": "str",
        "NAME20": "str",
        "NAMELSAD": "str",
        "NAMELSAD20": "str",
        "NAMELSADCO": "category",
        "NCTADVFP": "int",
        "NECTAFP": "int",
        "PARTFLG": "category",
        "PLACEFP": "int",
        "PLACENS": "int",
        "REGIONCE": "int",
        "SCSDLEA": "int",
        "SLDLST": "str",
        "SLDUST": "str",
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
        "VTDST20": "str",
    }
    for name, dtype in mapping.items():
        try:
            gdf[name] = gdf[name].astype(dtype, errors="ignore")
        except KeyError:
            pass

    #write to parquet
    outputname = os.path.join(os.path.dirname(filename), 'boundary_outputs', os.path.splitext(os.path.split(filename)[-1])[0]+ '.parquet')
    ddf = dask_geopandas.from_geopandas(gdf, npartitions=1)
    ddf.to_parquet(outputname)
    print(f'Finished {outputname}')
    return filename


def main():
    files = glob.glob('./census_boundaries/*.zip')
    print(f'Found {len(files)} files')
    bg = bag.from_sequence(files).map(load_dtype)
    bg.compute()


if __name__ == '__main__':
    main()
