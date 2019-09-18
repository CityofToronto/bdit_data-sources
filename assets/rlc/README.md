# Red Light Cameras dataset

This is one of the datasets managed by the Automated Speed Enforcement group (https://github.com/CityofToronto/bdit_vz_programs#datasets-and-their-owners).  

Data is updated by automatic feed to Open Data, available from Open Data API in json format (see
https://secure.toronto.ca/opendata/cart/red_light_cameras/details.html). Here, we set up an airflow process to automatically extract RLC data from the Open Data API and update the table in the bigdata RDS. The pipeline consists of the following steps:  

## 1. Create a Table in Local RDS  
This is a one-off task done in pgAdmin. Create a database called `rlc` in the Local RDS and in it create a table `rlc` in the public schema that contains the same columns as table `rlc` in `vz_safety_programs_staging` in the bigdata RDS.  

## 2. Create airflow process to pull Open Data data into bigdata RDS  
The airflow process `rlc.py` defines a task that executes two tasks: the first runs the following `psql` command to truncate the Table `vz_safety_programs_staging.rlc` in the bigdata RDS:  

```
psql -v ON_ERROR_STOP=1 -U vzairflow -h 10.160.12.47 -p 5432 bigdata -c "TRUNCATE vz_safety_programs_staging.rlc;"
```
The second task pulls the Red Light Camera data from the Open Data API and stores it directly into `vz_safety_programs_staging.rlc` in the bigdata RDS:  

```
db_name='bigdata'
username='vzairflow'
host='10.160.12.47'
local_table='vz_safety_programs_staging.rlc'
# Connect to bigdata RDS
conn = psycopg2.connect(database=db_name, user=username, host=host, port=5432)

url = "https://secure.toronto.ca/opendata/cart/red_light_cameras.json"
return_json = requests.get(url).json()
rows = [list(feature.values()) for feature in return_json]
insert = 'INSERT INTO {0} VALUES %s'.format(local_table)
with conn:
  with conn.cursor() as cur:
    execute_values(cur, insert, rows)
    print(rows)

conn.close()
```
