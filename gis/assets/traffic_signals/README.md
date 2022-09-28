# Traffic Signals dataset

This is one of the datasets managed by the Traffic Control group (https://github.com/CityofToronto/bdit_vz_programs#datasets-and-their-owners).  

Data is updated by automatic feed in Traffic Control and stored in their Oracle database. Here, we set up an airflow process to automatically extract Traffic Signal data from the Oracle database using Foreign Data Wrappers and update the table in the bigdata RDS. The pipeline consists of the following steps:  

## 2. Create airflow process to copy view into bigdata RDS  
The airflow process `traffic_signals.py` defines a task that executes the following `psql` command to copy the View created in Step 1 on `localhost` to Table `vz_safety_programs_staging.signals_cart` in the bigdata RDS. Table `signals_cart` gets truncated each time the script is called.  

```
SET -o pipefail;
psql -U airflow -h localhost -p 5432 traffic_signals -c "COPY (SELECT * FROM public.signals_cart) TO STDOUT (FORMAT text, ENCODING 'UTF-8')" | psql $vz_pg_uri -v "ON_ERROR_STOP=1" -c "TRUNCATE vz_safety_programs_staging.signals_cart; COPY vz_safety_programs_staging.signals_cart FROM STDIN;"
```
