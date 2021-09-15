from setuptools import setup

setup(
    name='census-parquet',
    version='0.0.1',
    packages=['census_parquet'],
    license='MIT',
    description='Tools for generating Parquet files from Census 2020',
    author='makepath',
    url='https://github.com/makepath/census-parquet',
    install_requires=[
        'dask_geopandas',
        'openpyxl',
        'pyarrow',
    ],
)
