# Traffic Signals dataset 

Data is updated by pulling GeoJSON files from City of Toronto Open Data.

- Red Light Cameras: https://open.toronto.ca/dataset/red-light-cameras/
- Other traffic signals: https://open.toronto.ca/dataset/traffic-signals-tabular/

Here, we set up an airflow process to automatically extract Traffic Signals data from OpenAPI and update the relevant tables in the bigdata RDS. The pipeline is in `pull_traffic_signals.py` and it pulls multiple types of traffic signals to tables `vz_safety_programs_staging.rlc`, `vz_safety_programs_staging.signals_cart`, and `gis.traffic_signal`.

## Where do each type of traffic signal gets sent?

vz_safety_programs_staging.rlc
- Red Light Cameras (RLC)

vz_safety_programs_staging.signals_cart
- Pedestrian Head Start Signals/Leading Pedestrian Intervals (LPI)
- Accessible Pedestrian Signals (APS)
- Pedestrian Crossovers (PXO)
- Traffic Signals

gis.traffic_signal (audited table)
- Traffic Signals

## RLC
URL: https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/9fcff3e1-3737-43cf-b410-05acd615e27b/resource/7e4ac806-4e7a-49d3-81e1-7a14375c9025/download/Red%20Light%20Cameras%20Data.geojson

Every time a new version of RLC dataset is pulled, table `vz_safety_programs_staging.rlc` would get truncated beforehand.

## APS and LPI
URL: https://secure.toronto.ca/opendata/cart/traffic_signals/v3?format=json

The script loops through each traffic signal record in the URL json and would only pull those that are indicated to have APS/LPI installed. Then, existing APS/LPI records in `vz_safety_programs_staging.signals_cart` would be deleted to clear space for a new version of dataset.

## PXO
URL: https://secure.toronto.ca/opendata/cart/pedestrian_crossovers/v2?format=json

Similar to APS & LPI, just that the URL is different.

## Traffic Signals (general)
URL: https://secure.toronto.ca/opendata/cart/traffic_signals/v3?format=json

Every record from this URL will end up in `gis.traffic_signal` and `vz_safety_programs_staging.signals_cart`. For `signals_cart`, existing records of traffic signals will first be deleted and then new ones inserted. For `traffic_signal`, the script will perform upsert instead so that the changes could be audited.

