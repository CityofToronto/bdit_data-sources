# Red Light Cameras dataset

This is one of the datasets managed by the Automated Speed Enforcement group (https://github.com/CityofToronto/bdit_vz_programs#datasets-and-their-owners).  

Data is updated by automatic feed to Open Data, available from Open Data API in json format (see
https://secure.toronto.ca/opendata/cart/red_light_cameras/details.html). Here, we set up an airflow process to automatically extract RLC data from the Open Data API and update the table in the bigdata RDS. 

This is run nightly in a DAG called [`assets_pull.py`](bdit_data-sources/dags/assets_pull.py)
