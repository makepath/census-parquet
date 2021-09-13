#!/bin/bash
mkdir -p census_boundaries
touch boundaryurl.txt
echo -e "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_all_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_aiannh_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_aitsn_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_02_anrc_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_tbg_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_ttract_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_bg_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_tract_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_cd116_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_concity_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_within_cd116_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_cousub_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_division_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_cbsa_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_csa_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_metdiv_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_necta_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_nectadiv_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_cnecta_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_place_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_region_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_elsd_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_scsd_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_unsd_500k.zip
\nhttps://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_sldl_500k.zip" >> boundaryurl.txt
wget -P census_boundaries/ -i boundaryurl.txt
