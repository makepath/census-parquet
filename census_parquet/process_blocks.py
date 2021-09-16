import dask.bag as bag
import dask_geopandas
import geopandas as gpd
import glob
import numpy as np
import os
import pandas as pd
import pyarrow.parquet as pq


statelookup = {'01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI', '56': 'WY', '72': 'PR'}


SUMMARY_TABLE = "./population_stats/2020_PLSummaryFile_FieldNames.xlsx"


def add_population_stats(filename, gdf):

    FIPS = os.path.split(filename)[-1].split('_')[2]
    ABBR = statelookup.get(FIPS)

    if not ABBR:
        print(f'unable to parse FIPS:{FIPS}')
        gdf['P0010001'] = np.empty(len(gdf), dtype='f8')
        gdf['STUSAB'] = ABBR
        return gdf

    state_1 = f'./population_stats/{ ABBR.lower() }000012020.pl'
    state_geo = f'./population_stats/{ ABBR.lower() }geo2020.pl'

    if not os.path.exists(state_1) or not os.path.exists(state_geo):
        print(f' unable to find {state_1}')
        gdf['P0010001'] = np.zeros(len(gdf), dtype='f8') * np.nan
        gdf['STUSAB'] = ABBR
        return gdf

    seg_1_header_df = pd.read_excel(
        SUMMARY_TABLE,
        sheet_name="2020 P.L. Segment 1 Fields"
    )

    geo_header_df = pd.read_excel(
        SUMMARY_TABLE,
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

    pop_df = pd.merge(
        left=geo_df[["LOGRECNO", "GEOID", "STUSAB", "BLOCK", "SUMLEV"]],
        right=seg_1_df[["LOGRECNO", "P0010001"]],
        how="left",
        on="LOGRECNO"
    )

    block_df = pop_df[pop_df.SUMLEV == 750]

    block_df['GEOID'] = block_df['GEOID'].str.replace("7500000US", "")

    updated_gdf = pd.merge(
        left=gdf,
        right=block_df,
        how="left",
        left_on="GEOID20",
        right_on="GEOID"
    )

    updated_gdf['STUSAB'] = updated_gdf['STUSAB'].astype('category')
    updated_gdf['P0010001'] = updated_gdf['P0010001'].astype(float)

    del updated_gdf['LOGRECNO']
    del updated_gdf['BLOCK']
    del updated_gdf['SUMLEV']
    del updated_gdf['GEOID']

    return updated_gdf


def load(filename):
    print(f'Started {filename}')
    census_geom = gpd.read_file(filename, driver='SHP')
    gdf = add_population_stats(filename, census_geom)

    # dtype conversions
    gdf['INTPTLON20'] = gdf['INTPTLON20'].astype(float)
    gdf['INTPTLAT20'] = gdf['INTPTLAT20'].str.replace('+', '').astype(float)
    gdf['STATEFP20'] = gdf['STATEFP20'].astype('category')
    gdf['COUNTYFP20'] = gdf['COUNTYFP20'].astype('category')
    gdf['TRACTCE20'] = gdf['TRACTCE20'].astype('int')
    gdf['BLOCKCE20'] = gdf['BLOCKCE20'].astype('int')

    # drop fields
    del gdf['MTFCC20']
    del gdf['UR20']
    del gdf['UACE20']
    del gdf['UATYPE20']
    del gdf['FUNCSTAT20']

    # write to parquet
    outputname = os.path.join('./outputs', os.path.split(filename)[-1]+'.parquet')
    ddf = dask_geopandas.from_geopandas(gdf, npartitions=1)
    ddf.to_parquet(outputname)
    print(f'Finished {outputname}')
    return filename


def combine_parquet_files(input_folder, target_path):
    try:
        files = []

        for file_name in os.listdir(input_folder):
            files.append(pq.read_table(os.path.join(input_folder, file_name)))

        for f in files:
            pq.write_to_dataset(
                f,
                root_path=target_path,
                partition_cols=['STATEFP20','COUNTYFP20'])

    except Exception as e:
        print(e)
        raise


def main():
    files = glob.glob('./TABBLOCK20/*.zip')
    bg = bag.from_sequence(files).map(load)
    bg.compute()
    combine_parquet_files('./outputs', 'tl_2020_FULL_tabblock20.parquet')


if __name__ == '__main__':
    main()
