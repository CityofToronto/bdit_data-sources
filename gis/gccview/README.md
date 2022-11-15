# GCCVIEW pipeline

## Overview

The GCC (Geospatial Competency Centre) offers a number of spatial products on their arcgis rest api. They host 26 map servers and has over [300 layers](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/available_layers.md) from different divisions within the City of Toronto. To ensure the geospatial layers we use are the most up to date at a regular basis, we are creating a data pipeline to automate the process of retrieving layers from gccview to our postgresql database and conflate street networks through sharedstreets.

Since we can't access the GCC rest API outside the City's firewall, the pipeline has to run in the on-premise server (Morbius / Bancroft) environment, using Airflow 2.1.4.

## Where the layers are pulled

The GCC pipeline will be pulling layers we want into the `gis_core` and `gis` schemas in the EC2 `bigdata` database, as well as the `gis` schema in the Bancroft `ptc` database.

### gis (on-prem)

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
|traffic_camera|2|3|
|permit_parking_area|2|11|
|prai_transit_shelter|2|35|
|traffic_bylaw_point|2|38|
|traffic_bylaw_line|2|39|
|loop_detector|2|46|
|electrical_vehicle_charging_station|20|1|
|day_care_centre|22|1|
|middle_childcare_centre|22|2|
|business_improvement_area|23|1|
|proposed_business_improvement_area|23|13|
|film_permit_all|23|9|
|film_permit_parking_all|23|10|
|hotel|23|12|
|convenience_store|26|1|
|supermarket|26|4|
|place_of_worship|26|5|
|ymca|26|6|
|aboriginal_organization|26|45|
|attraction|26|46|
|dropin|26|47|
|early_years_centre|26|48|
|family_resource_centre|26|49|
|food_bank|26|50|
|longterm_care|26|53|
|parenting_family_literacy|26|54|
|retirement_home|26|58|
|senior_housing|26|59|
|shelter|26|61|
|social_housing|26|62|
|private_road|27|13|
|school|28|17|
|library|28|28|

## How the script works

The pipeline consists of two files, `gcc_layer_refresh.py` for the functions and `gcc_layers_pull.py` for the Airflow DAG. The main function that fetches the layers is called `get_layer` and it takes in five parameters. Here is a list that describes what each parameter means:

- mapserver_n (int): ID of the mapserver that host the desired layer
- layer_id (int): ID of the layer within the particular mapserver
- schema_name (string): name of destination schema
- is_audited (Boolean): True if the layer will be in a table that is audited, False if the layer will be inserted as a child table part of a parent table
- cred (Airflow PostgresHook): the credentials to enable a connection to a particular database

In the DAG file, the arguments for each layer is stored in a dictionary called "bigdata_layers" or "morbius_layers", in the order of the parameters above. The DAG will be executed once every XXX days. 


## Manually fetch layers

Currently you can use [this notebook](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/get_layer_gccview.ipynb) on Morbius server environment to fetch layer from gccview rest api and send it to postgresql in the schema you want.

Look for the mapserver that host the layer, and the layer id from the tables above to find the layer you want. Set the variables accordingly.
