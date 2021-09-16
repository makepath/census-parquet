#!/bin/bash
wget -w 0.5 -c -r -np -nH -nv -e robots=off -R "index.html*" --cut-dirs=3 https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/
