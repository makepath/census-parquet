# census-parquet
Python tools for creating and maintaining Parquet files from [2020 Census Data](https://www.census.gov/programs-surveys/decennial-census/decade/2020/2020-census-main.html).


## Install Dependencies
These tools utilize several dependencies.

To utilize the data download shell script files install [wget](https://formulae.brew.sh/formula/wget) and [lftp](https://formulae.brew.sh/formula/lftp).
```bash
brew install wget
brew install lftp                                                                                                                                 
```
For the python scripts the following dependencies should be installed:
```bash
pip install dask
pip install dask_geopandas
pip install geopandas
pip install numpy
pip install pandas
pip install pyarrow
```
## Usage
The scripts should be run in the following order.

First, run the three shell scripts which download all the data needed for running the python scripts. 
1. `download_boundaries.sh` - This script downloads the Census Boundary data needed to run boundary_processing.py
2. `download_population_stats.sh` - This script downloads population stat data needed for process_blocks.py
3. `download_blocks.sh` - This script downloads the Census Block data needed to run process_blocks.py

After running the shell scripts you can then run the python scripts.
1. `boundary_processing.py` - This script processes the Census Boundary data and creates parquet files
2. `process_blocks.py` - This script processes Census Block data and creates parquet files

