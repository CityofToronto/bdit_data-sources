# GCCVIEW pipeline

## Overview
The GCC (Geospatial Competency Centre) offers a number of spatial products on their arcgis rest api. They host 26 map servers and has over [300 layers](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/available_layers.md) from different divisions within the City of Toronto. To ensure the geospatial layers we use are the most up to date at a regular basis, we are creating a data pipeline to automate the process of retrieving layers from gccview to our postgresql database and conflate street networks through sharedstreets.

Since we can't access the GCC rest API outside the City's firewall, the pipeline has to run in the on-premise server (Morbius) environment, using Airflow 2.1.4.

## Where to find the layers
The GCC pipeline will be pulling layers we want into the `gis_core` and `gis` schemas in the EC2 `bigdata` database, as well as the `gis` schema in the Morbius `ptc` database.

### gis (Morbius)

|Layer Name|Mapserver ID|Layer ID|
|-|-|-|
|city_ward|0|0|
|centreline|0|2|
|intersection|12|42|
|centreline_intersection_point|0|19|
|ibms_district|11|23|
|ibms_grid|11|25|

### gis_core (bigdata)

|Layer Name|Mapserver ID|Layer ID|
|-|-|-|
|city_ward|0|0|
|centreline|0|2|
|intersection|12|42|
|centreline_intersection_point|0|19|
|census_tract|26|7|
|neighbourhood_improvement_area|26|11|
|priority_neighbourhood_for_investment|26|13|

### gis (bigdata)

|Layer Name|Mapserver ID|Layer ID|
|-|-|-|
|bikeway|2|2|

## How the script works

Lorem ipsum dolor sit amet. Ut perferendis esse vel dicta odit id facere modi. Et ipsum dolores ut vitae perferendis est porro doloribus qui dignissimos perspiciatis 33 impedit expedita. Sit illo dolores ut facilis aliquam qui porro sequi sit quis consequatur in adipisci doloremque et magnam eaque! Aut iusto possimus et omnis voluptatem eum quia asperiores aut consequatur consectetur At dolores minus.

## Manually fetch layers
Currently you can use [this notebook](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/get_layer_gccview.ipynb) on your local machine to fetch layer (currently support: bikeway, centreline and bylaw) from gccview rest api and send it to postgresql in schema `gis`. Make sure you have all the packages installed and [set the path](https://github.com/CityofToronto/bdit_team_wiki/wiki/postgresql#from-python) to your config file. 

Look for the mapserver that host the layer, and the layer id from [avaliable layers](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/available_layers.md), and run `get_layer(themapserver, id)`. Example: `get_layer(2, 2)` for bikeway layer.
