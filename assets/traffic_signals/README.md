# Traffic Signals dataset

This is one of the datasets managed by the Traffic Control group (https://github.com/CityofToronto/bdit_vz_programs#datasets-and-their-owners).  

Data is updated by automatic feed in Traffic Control and stored in their Oracle database. Here, we set up an airflow process to automatically extract Traffic Signal data from the Oracle database using Foreign Data Wrappers and update the table in the bigdata RDS. The pipeline consists of the following steps:  

## 1. Create a View in Local RDS  
This is a one-off task done in pgAdmin. First, create a database called `traffic_signals` in the Local RDS and in it create 5 foreign tables in `public` schema under `Foreign Tables` using the [Foreign Data Wrapper](#https://github.com/CityofToronto/bdit_team_wiki/wiki/Automating-Stuff#Foreign-Data-Wrapper-for-Oracle-tables-in-Linux): `lbomaingeneral`, `sgmaingeneral`, `sgpxgenmaingeneral`, `sgsimaingeneral`, `upsmaingeneral`, which wrap tables of the same name in the Signal View Oracle database `cartpd` under schema `CARTEDBA`.    

Next, created a View called `signals_cart` in `traffic_signals.public.Views` using the following PostgreSQL query on the foreign tables:  

```
SELECT 'Traffic Signals'::text AS asset_type,
   a.id::integer AS px,
   a.streetname AS main_street,
   a.midblockroute AS midblock_route,
   a.side1routef AS side1_street,
   a.side2route AS side2_street,
   b.latitude,
   b.longitude,
   b.activationdate AS activation_date,
   NULL::text AS details
  FROM sgmaingeneral a
    JOIN sgpxgenmaingeneral b ON a.sgmaingeneraloid = b.sgmaingeneraloid
 WHERE a.id::integer < 5000 AND b.activationdate IS NOT NULL AND b.removaldate IS NULL
UNION ALL
SELECT 'Pedestrian Crossovers'::text AS asset_type,
   a.id::integer AS px,
   a.streetname AS main_street,
   a.midblockroute AS midblock_route,
   a.side1routef AS side1_street,
   a.side2route AS side2_street,
   b.latitude,
   b.longitude,
   b.activationdate AS activation_date,
   NULL::text AS details
  FROM sgmaingeneral a
    JOIN sgpxgenmaingeneral b ON a.sgmaingeneraloid = b.sgmaingeneraloid
 WHERE a.id::integer >= 5000 AND a.id::integer < 7000 AND b.activationdate IS NOT NULL AND b.removaldate IS NULL
UNION ALL
SELECT 'Audible Pedestrian Signals'::text AS asset_type,
   a.id::integer AS px,
   a.streetname AS main_street,
   a.midblockroute AS midblock_route,
   a.side1routef AS side1_street,
   a.side2route AS side2_street,
   b.latitude,
   b.longitude,
   c.apsactivationdate AS activation_date,
   NULL::text AS details
  FROM sgmaingeneral a
    JOIN sgpxgenmaingeneral b ON a.sgmaingeneraloid = b.sgmaingeneraloid
    JOIN sgsimaingeneral c ON a.sgmaingeneraloid = c.sgmaingeneraloid
 WHERE c.apsactivationdate IS NOT NULL
UNION ALL
SELECT 'Uninterruptable Power Supply'::text AS asset_type,
   a.id::integer AS px,
   a.streetname AS main_street,
   a.midblockroute AS midblock_route,
   a.side1route AS side1_street,
   a.side2route AS side2_street,
   a.latitude::numeric AS latitude,
   a.longitude::numeric AS longitude,
   a.activationdate AS activation_date,
   NULL::text AS details
  FROM upsmaingeneral a
 WHERE a.activationdate IS NOT NULL
UNION ALL
SELECT 'Leading Pedestrian Intervals'::text AS asset_type,
   a.id::integer AS px,
   a.streetname AS main_street,
   a.midblockroute AS midblock_route,
   a.side1routef AS side1_street,
   a.side2route AS side2_street,
   b.latitude,
   b.longitude,
   c.lpiactivationdate AS activation_date,
   NULL::text AS details
  FROM sgmaingeneral a
    JOIN sgpxgenmaingeneral b ON a.sgmaingeneraloid = b.sgmaingeneraloid
    JOIN sgsimaingeneral c ON a.sgmaingeneraloid = c.sgmaingeneraloid
 WHERE c.lpiactivationdate IS NOT NULL
UNION ALL
SELECT 'LED Blankout Signs'::text AS asset_type,
   a.id::integer AS px,
   a.streetname AS main_street,
   a.midblockroute AS midblock_route,
   a.side1route AS side1_street,
   a.side2route AS side2_street,
   a.latitude::numeric AS latitude,
   a.longitude::numeric AS longitude,
   a.activationdate AS activation_date,
   (a.lboapproach::text || ' '::text) || a.lborestrictioin::text AS details
  FROM lbomaingeneral a
 ORDER BY 1, 9;
```

## 2. Create airflow process to copy view into bigdata RDS  
The airflow process `traffic_signals.py` defines a task that executes the following `psql` command to copy the View created in Step 1 on `localhost` to Table `vz_safety_programs_staging.signals_cart` in the bigdata RDS. Table `signals_cart` gets truncated each time the script is called.  

```
psql -U airflow -h localhost -p 5432 traffic_signals -c "COPY (SELECT * FROM public.signals_cart) TO STDOUT (FORMAT text, ENCODING 'UTF-8')" | psql -v ON_ERROR_STOP=1 -U vzairflow -h 10.160.12.47 -p 5432 bigdata -c "TRUNCATE vz_safety_programs_staging.signals_cart; COPY vz_safety_programs_staging.signals_cart FROM STDIN;"
```
