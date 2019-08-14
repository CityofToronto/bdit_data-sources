# GCCVIEW pipeline

The GCC (Geospatial Competency Centre) offers a number of spatial products on their arcgis rest api. They host 26 map servers and has over [300 layers](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/available_layers.md) from different divisions within the City of Toronto. To ensure the geospatial layers we use are the most up to date at a regular basis, we are creating a data pipeline to automate the process of retrieving layers from gccview to our postgresql database and conflate street networks through sharedstreets.

Since we can't access the GCC rest API outside the City's firewall, we have to seperate the process into two stages. Retrieve the layer from GCCVIEW and send to postgresql in a local machine, then send to SharedStreets for matching on the EC2. 

Currently you can use [this notebook](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/get_layer_gccview.ipynb) on your local machine to fetch layer (currently support: bikeway, centreline and bylaw) from gccview rest api and send it to postgresql in schema `gis`. Make sure you have all the packages installed and [set the path](https://github.com/CityofToronto/bdit_team_wiki/wiki/postgresql#from-python) to your config file. 

Look for the mapserver that host the layer, and the layer id from [avaliable layers](https://github.com/CityofToronto/bdit_data-sources/blob/gcc_view/gis/gccview/available_layers.md), and run `get_layer(themapserver, id)`. Example: `get_layer(2, 2)` for bikeway layer.
