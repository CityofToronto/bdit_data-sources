# GCCVIEW pipeline
  * [Overview](#overview)
  * [Where the layers are pulled](#where-the-layers-are-pulled)
  * [How the script works](#how-the-script-works)
  * [Adding new layers to GCC Puller DAG](#adding-new-layers-to-gcc-puller-dag)
  * [Manually fetch layers - Using Jupyter Notebook](#manually-fetch-layers---using-jupyter-notebook)
  * [Manually fetch layers - Using Click in command prompt](#manually-fetch-layers---using-click-in-command-prompt)



## Overview

The GCC (Geospatial Competency Centre) offers a number of spatial products on their arcgis rest api. They host 26 map servers and has over [300 layers](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/available_layers.md) from different divisions within the City of Toronto (note that `available_layers.md` might be outdated). To ensure the geospatial layers we use are the most up to date at a regular basis, we are creating a data pipeline to automate the process of retrieving layers from gccview to our postgresql database and conflate street networks through sharedstreets.

Since there are layers that cannot be accessed from the GCC's external REST API, the pipeline has to run on premises, e.g., Morbius and/or Bancroft.

## Where the layers are pulled

The GCC pipeline will be pulling multiple layers into the `gis_core` and `gis` schemas in the `bigdata` database hosted by Amazon RDS, as well as the `gis` schema in `ptc` database hosted on the on-prem server, Bancroft.

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
|cycling_infrastructure|2|49|
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

The pipeline consists of two files, `gcc_puller_functions.py` for the functions and `gcc_layers_pull.py` for the Airflow DAG. The main function that fetches the layers is called `get_layer` and it takes in five parameters. Here is a list that describes what each parameter means:

- mapserver_n (int): ID of the mapserver that host the desired layer
- layer_id (int): ID of the layer within the particular mapserver
- schema_name (string): name of destination schema
- is_audited (Boolean): True if the layer will be in a table that is audited, False if the layer will be inserted as a child table part of a parent table
- cred (Airflow PostgresHook): the Airflow PostgresHook that directs to credentials to enable a connection to a particular database

In the DAG file, the arguments for each layer are stored in dictionaries called "bigdata_layers" and "ptc_layers", in the order above. The DAG will be executed once every 3 months, particularly on the 15th of every March, June, September, and December every year.

## Adding new layers to GCC Puller DAG
1. Identify the mapserver_n and layer_id for the layer you wish to add. You can find COT transportation layers here: https://insideto-gis.toronto.ca/arcgis/rest/services/cot_geospatial2/FeatureServer, where mapserver_n is 2 and the layer_id is in brackets after the layer name.
2. Add a new entry to "bigdata_layers" or "ptc_layers" dictionaries in [gcc_layers_pull.py](/dags/gcc_layers_pull.py) depending on the destination database. 
3. If is_audited = True, you must also add a primary key for the new layer to "pk_dict" in [gcc_puller_functions.py](gcc_puller_functions.py).

## Manually fetch layers - Using Jupyter Notebook

One option is to use [this notebook](./gcc_puller.ipynb) on Morbius server environment to fetch layer from gccview rest api and send it to postgresql in the schema you want.

To use the Jupyter notebook:
1. Know the name of the layer you want to fetch. 
2. Look for the mapserver that host the layer, and the layer id using the tables above.
3. Determine the schema of where you want the downloaded table to be.
4. Enter the .cfg file path at the 'Config' code block.
5. Enter the variables using the pre-existing template code block provided at the end of the notebook file.
6. Execute the code blocks from top to bottom.
7. Open pgAdmin, go to the specified schema and check if the layer's information had been pulled correctly.

Note that if you want to pull a partitioned child table into your personal schema, you need to set up the parent table first. Refer to the .sql files in `/gis/gccview/sql`.

## Manually fetch layers - Using Click in command prompt

The second option is to execute `gcc_puller_functions.py` in command prompt (or venv in Morbius or other servers).

There are 5 inputs that need to be entered, which are very similar to the ones listed above for function `get_layer`. The only difference is that the last parameter now needs to be a string that contains the path to your .cfg file.

Run the following line to see the details of how to enter each parameter with Click.

```python3 {FULL_PATH_TO_gcc_puller_functions.py} --help```

Note that if the script doesn't work, one reason might be because your credentials don't have access to the GCC API.
