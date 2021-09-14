# census-parquet
Python tools for creating and maintaining Parquet files from [US 2020 Census Data](https://www.census.gov/programs-surveys/decennial-census/decade/2020/2020-census-main.html).


## Install Dependencies
These tools utilize several dependencies.

To utilize the data download shell script files install [wget](https://en.wikipedia.org/wiki/Wget) and [lftp](https://en.wikipedia.org/wiki/Lftp).

For the python scripts the following dependencies should be installed:
1. [dask](https://docs.dask.org/en/latest/install.html)
2. [dask_geopandas](https://github.com/geopandas/dask-geopandas)
3. [geopandas](https://geopandas.org/getting_started/install.html)
4. [numpy](https://numpy.org/install/)
5. [pandas](https://pandas.pydata.org/docs/getting_started/install.html)
6. [pyarrow](https://arrow.apache.org/docs/python/install.html)

## Usage
The scripts should be run in the following order.

First, run the three shell scripts which download all the data needed for running the python scripts: 
1. `download_boundaries.sh` - This script downloads the Census Boundary data needed to run boundary_processing.py
2. `download_population_stats.sh` - This script downloads population stat data needed for process_blocks.py
3. `download_blocks.sh` - This script downloads the Census Block data needed to run process_blocks.py

After running the shell scripts you can then run the python scripts:
1. `boundary_processing.py` - This script processes the Census Boundary data and creates parquet files. The parquet files will be output into a `boundary_outputs` folder. 
2. `process_blocks.py` - This script processes Census Block data and creates parquet files. The final combined parquet file will have the name `tl_2020_FULL_tabblock20.parquet`.
 
