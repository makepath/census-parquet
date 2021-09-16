#!/bin/bash
mkdir -p census_boundaries
cd census_boundaries
wget https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_all_500k.zip
unzip cb_2020_us_all_500k.zip
rm cb_2020_us_all_500k.zip
