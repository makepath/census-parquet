# census-parquet
Python tools for creating and maintaining Parquet files from [US 2020 Census Data](https://www.census.gov/programs-surveys/decennial-census/decade/2020/2020-census-main.html).


## Installation

To use the data download shell script files first install [wget](https://en.wikipedia.org/wiki/Wget).

To install the census-parquet package use
```
pip install census-parquet
```

This will also install the required Python dependencies which are:
1. [click](https://github.com/pallets/click)
2. [dask](https://docs.dask.org/en/latest/install.html)
3. [dask_geopandas](https://github.com/geopandas/dask-geopandas)
4. [geopandas](https://geopandas.org/getting_started/install.html)
5. [numpy](https://numpy.org/install/)
6. [openpyxl](https://openpyxl.readthedocs.io/en/stable/#installation)
7. [pandas](https://pandas.pydata.org/docs/getting_started/install.html)
8. [pyarrow](https://arrow.apache.org/docs/python/install.html)

## Usage
To run the census-parquet code simply use
```
run_census_parquet
```

This runs the following scripts in order:
1. `download_boundaries.sh` - This script downloads the Census Boundary data needed to run boundary_processing.py
2. `download_population_stats.sh` - This script downloads population stat data needed for process_blocks.py
3. `download_blocks.sh` - This script downloads the Census Block data needed to run process_blocks.py
4. `boundary_processing.py` - This script processes the Census Boundary data and creates parquet files. The parquet files will be output into a `boundary_outputs` folder.
5. `process_blocks.py` - This script processes Census Block data and creates parquet files. The final combined parquet file will have the name `tl_2020_FULL_tabblock20.parquet`.
