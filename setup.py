from setuptools import setup

setup(
    name='census-parquet',
    version='0.0.9',
    packages=['census_parquet'],
    description='Tools for generating Parquet files from US Census 2020',
    author='makepath',
    url='https://github.com/makepath/census-parquet',
    entry_points={
        'console_scripts': ['run_census_parquet=census_parquet.cli:start']
    },
    install_requires=[
        'click',
        'dask_geopandas',
        'openpyxl',
        'pyarrow',
    ],
    package_data={
        'census_parquet': ['*.sh'],
    },
)
