# Assets

An [airflow process](../../dags/assets_pull.py) to automatically extract Open Data on assets by pulling GeoJSON files from City of Toronto Open Data. Currently pulling two datasets

- Red Light Cameras: https://open.toronto.ca/dataset/red-light-cameras/
- Traffic signals: https://open.toronto.ca/dataset/traffic-signals-tabular/

## Where does each type of asset data get sent?

`gis.traffic_signal` (audited table): stores the traffic signal data in the wide format from Open Data

- [`pull_traffic_signal()`](#traffic-signal-data): Traffic Signals

`vz_safety_programs_staging.rlc`

- [`pull_rlc()`](#rlc): Red Light Cameras (RLC)

`vz_safety_programs_staging.signals_cart`: stores data by traffic signal asset type for the purposes of the Vision Zero Map & Dashboard. Instead of the wide format from Open Data, each of the below types has its own row.

- [`pull_lpi()`](#aps-and-lpi): Pedestrian Head Start Signals/Leading Pedestrian Intervals (LPI)
- [`pull_aps()`](#aps-and-lpi): Accessible Pedestrian Signals (APS)
- [`pull_pxo()`](#pxo): Pedestrian Crossovers (PXO)
- [`pull_traffic_signal()`](#traffic-signal-data): Traffic Signals

## RLC

https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/9fcff3e1-3737-43cf-b410-05acd615e27b/resource/7e4ac806-4e7a-49d3-81e1-7a14375c9025/download/Red%20Light%20Cameras%20Data.geojson

Every time a new version of RLC dataset is pulled, table `vz_safety_programs_staging.rlc` would get truncated beforehand.

## Traffic Signal Data

https://secure.toronto.ca/opendata/cart/traffic_signals/v3?format=json

This is one of the [datasets managed by the Traffic Control group](https://github.com/CityofToronto/bdit_vz_programs#datasets-and-their-owners). 
 Traffic Signals data from Open and update the relevant tables in the bigdata RDS. 

Records from this URL are inserted into `gis.traffic_signal` and `vz_safety_programs_staging.signals_cart` as follows: 

`gis.traffic_signal`
- rows are upserted so that the changes can be audited.
- `removed_date` field notes when the row was removed from the Open Dataset. Note the field was first populated on 2024-10-09 so signals removed before that date will be rounded up to that date.
- `temp_signal` field notes temporary (px = '31XX', to be replaced by a permanent signal) and temporary (portable, typically for a work zone, and will be removed) (px = '33XX') traffic signals.

`signals_cart`
- existing records of traffic signals will first be deleted and then new ones inserted
- Temporary (portable) (px = '33XX') signals are exlcuded based on request from VZ Team. 

### APS and LPI

The `pull_lpi()` and `pull_aps()` functions loop through each traffic signal record in the JSON and only pull those that are indicated to have APS/LPI installed. Then, existing APS/LPI records in `vz_safety_programs_staging.signals_cart` are deleted to clear space for a new version of dataset.

## PXO
https://secure.toronto.ca/opendata/cart/pedestrian_crossovers/v2?format=json

Similar to [APS & LPI](#aps-and-lpi), but this pulls from a different dataset.

# Locally pulling from Oracle (archive)

Data is updated by automatic feed in Traffic Control and stored in their Oracle database. Here, we set up an airflow process to automatically extract Traffic Signal data from the Oracle database using Foreign Data Wrappers and update the table in the bigdata RDS. The pipeline consists of the following steps:  

## 1. Create a View in Local RDS  
This is a one-off task done in pgAdmin. First, create a database called `traffic_signals` in the Local RDS and in it create 5 foreign tables in `public` schema under `Foreign Tables` using the [Foreign Data Wrapper](#https://github.com/CityofToronto/bdit_team_wiki/wiki/Automating-Stuff#Foreign-Data-Wrapper-for-Oracle-tables-in-Linux): `lbomaingeneral`, `sgmaingeneral`, `sgpxgenmaingeneral`, `sgsimaingeneral`, `upsmaingeneral`, which wrap tables of the same name in the Signal View Oracle database `cartpd` under schema `CARTEDBA`.    

Next, created a View called `signals_cart` in `traffic_signals.public.Views` using the following PostgreSQL query on the foreign tables:  

```sql
SELECT
    'Traffic Signals' AS asset_type,
    a.id::integer AS px,
    a.streetname AS main_street,
    a.midblockroute AS midblock_route,
    a.side1routef AS side1_street,
    a.side2route AS side2_street,
    b.latitude,
    b.longitude,
    b.activation_date AS activation_date,
    NULL::text AS details
FROM sgmaingeneral AS a
JOIN sgpxgenmaingeneral AS b ON a.sgmaingeneraloid = b.sgmaingeneraloid
WHERE a.id::integer < 5000 AND b.activation_date IS NOT NULL AND b.removaldate IS NULL
UNION ALL
SELECT
    'Pedestrian Crossovers' AS asset_type,
    a.id::integer AS px,
    a.streetname AS main_street,
    a.midblockroute AS midblock_route,
    a.side1routef AS side1_street,
    a.side2route AS side2_street,
    b.latitude,
    b.longitude,
    b.activation_date AS activation_date,
    NULL::text AS details
FROM sgmaingeneral a
JOIN sgpxgenmaingeneral b ON a.sgmaingeneraloid = b.sgmaingeneraloid
WHERE 
    a.id::integer >= 5000 
    AND a.id::integer < 7000 
    AND b.activation_date IS NOT NULL 
    AND b.removaldate IS NULL
UNION ALL
SELECT
    'Audible Pedestrian Signals' AS asset_type,
    a.id::integer AS px,
    a.streetname AS main_street,
    a.midblockroute AS midblock_route,
    a.side1routef AS side1_street,
    a.side2route AS side2_street,
    b.latitude,
    b.longitude,
    c.apsactivation_date AS activation_date,
    NULL::text AS details
FROM sgmaingeneral a
JOIN sgpxgenmaingeneral b ON a.sgmaingeneraloid = b.sgmaingeneraloid
JOIN sgsimaingeneral c ON a.sgmaingeneraloid = c.sgmaingeneraloid
WHERE c.apsactivation_date IS NOT NULL
UNION ALL
SELECT 
    'Uninterruptable Power Supply' AS asset_type,
    a.id::integer AS px,
    a.streetname AS main_street,
    a.midblockroute AS midblock_route,
    a.side1route AS side1_street,
    a.side2route AS side2_street,
    a.latitude::numeric AS latitude,
    a.longitude::numeric AS longitude,
    a.activation_date AS activation_date,
    NULL::text AS details
FROM upsmaingeneral a
WHERE a.activation_date IS NOT NULL
UNION ALL
SELECT
    'Leading Pedestrian Intervals' AS asset_type,
    a.id::integer AS px,
    a.streetname AS main_street,
    a.midblockroute AS midblock_route,
    a.side1routef AS side1_street,
    a.side2route AS side2_street,
    b.latitude,
    b.longitude,
    c.lpiactivation_date AS activation_date,
    NULL::text AS details
FROM sgmaingeneral a
JOIN sgpxgenmaingeneral b ON a.sgmaingeneraloid = b.sgmaingeneraloid
JOIN sgsimaingeneral c ON a.sgmaingeneraloid = c.sgmaingeneraloid
WHERE c.lpiactivation_date IS NOT NULL
UNION ALL
SELECT
    'LED Blankout Signs' AS asset_type,
    a.id::integer AS px,
    a.streetname AS main_street,
    a.midblockroute AS midblock_route,
    a.side1route AS side1_street,
    a.side2route AS side2_street,
    a.latitude::numeric AS latitude,
    a.longitude::numeric AS longitude,
    a.activation_date AS activation_date,
    (a.lboapproach::text || ' '::text) || a.lborestrictioin::text AS details
FROM lbomaingeneral a
ORDER BY 1, 9;
```

## 2. Create airflow process to copy view into bigdata RDS  
The airflow process `traffic_signals.py` defines a task that executes the following `psql` command to copy the View created in Step 1 on `localhost` to Table `vz_safety_programs_staging.signals_cart` in the bigdata RDS. Table `signals_cart` gets truncated each time the script is called.  

```bash
SET -o pipefail;
psql -U airflow -h localhost -p 5432 traffic_signals -c "COPY (SELECT * FROM public.signals_cart) TO STDOUT (FORMAT text, ENCODING 'UTF-8')" | psql $vz_pg_uri -v "ON_ERROR_STOP=1" -c "TRUNCATE vz_safety_programs_staging.signals_cart; COPY vz_safety_programs_staging.signals_cart FROM STDIN;"
```
