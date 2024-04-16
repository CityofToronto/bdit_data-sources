# Traffic Analytics Data

- [Data Pipeline](#data-pipeline)
    - [Important Tables](#important-tables)
    - [Maintenance](#maintenance)
- [here_api.py](#here_apipy)
  - [Setup](#setup)
  - [Using the script](#using-the-script)
    - [download](#download)
    - [upload](#upload)
- [Loading New Data (Old Method)](#loading-new-data-old-method)

## Data Pipeline 

The HERE data pipeline pulls traffic data everyday at 4PM. Currently there are two daily automated airflow pipeline that pulls `PROBE_PATH` and `PATH` data respectively. 

#### PROBE_PATH

The [DAG](https://github.com/CityofToronto/bdit_data-sources/blob/master/dags/pull_here.py) that pulls `PROBE_PATH` data uses a BashOpertor to run [`here_api.py`](https://github.com/CityofToronto/bdit_data-sources/blob/master/here/traffic/here_api.py) on the scheduled time. It sends a request specifying the output and data resolution to the HERE API, unzips the file, pipes the output to `psql`, and runs the `/COPY` command to send the output to a simple view `here.ta_view`. The simple view has an insert trigger that executes the function [`here.here_insert_trigger`](https://github.com/CityofToronto/bdit_data-sources/blob/here_declarative/here/traffic/sql/trigger_here_insert.sql) for each row to redirect and transform input data to the parent `here.ta` table which will then insert into the corresponding partitioned table based on the input date. 
Probe path data include speed data derived from the paths travelled between probe points, on top of probe data derived from GPS probe speed observation.  

#### PATH

The [DAG](https://github.com/CityofToronto/bdit_data-sources/blob/master/dags/pull_here_path.py) that pulls `PATH` only data uses taskflow to run several functions in [`here_api_path.py`](https://github.com/CityofToronto/bdit_data-sources/blob/master/here/traffic/here_api_path.py) on the schedule time, which separates the steps of getting access token, requesting data, fetching the download url and copying the data with `curl` to send the output to a simple view `here.ta_path_view`. The simple view has an insert trigger that executes the function [`here.here_insert_trigger`](https://github.com/CityofToronto/bdit_data-sources/blob/here_declarative/here/traffic/sql/trigger_here_insert.sql) for each row to redirect and transform input data to the parent `here.ta_path` table which will then insert into the corresponding partitioned table based on the input date.
Path data only includes path data derived from the paths travelled between probe points and does not include speed derived from GPS probe speed observation.

### Important Tables

`here.ta`

All Traffic Analytics speed data are stored in partitioned tables under the parent `here.ta`. As we upgraded to PostGreSQL version 10, we were able to transition from inheritance based partitioning to declarative partitioning, see [this PR](https://github.com/CityofToronto/bdit_data-sources/pull/497) for more details on the transitioning process. Declarative partitioning allows for an easier setup and requires less maintenance. `here.ta` use range partitioning and the partition key is `dt`. Data is then partitioned by month by specifying the date range bounds.   

`here.ta_view`

This is a simple view created to redirect data to `here.ta` using triggers. Other than additional column `epoch_min` and the lack of columns `dt` and `tod`, it has all the columns in `here.ta`. An trigger with `INSTEAD OF INSERT` statement is created to redirect data received from psql command `/COPY` to `here.ta` along with transformation of `tx` (timestamp) to `dt` (date) and `tod` (time).

`here.ta_yyyymm`

These are partitioned tables of `here.ta`, named with the suffix of  `_yyyymm` (e.g. Data for August 2020 will be in table `here.ta_202008`). 

## Maintenance

After we transition to declarative partitioning, we would only need to create new monthly tables once every year. The function [`create_yearly_tables(yyyy text)`](https://github.com/CityofToronto/bdit_data-sources/blob/master/here/traffic/sql/function_create_yearly_tables.sql) was created for creating monthly partition tables for the input year. There is an [end of year airflow DAG](https://github.com/CityofToronto/bdit_data-sources/blob/master/dags/eoy_create_tables.py) dedicated for all our yearly maintenance which runs this function among others once a year.   
## here_api.py

### Setup

You should be able to do `pip install --user -e . --process-dependency-links` from within the folder.

Alternatively you can install using `pipenv` with `pipenv install -e "git+https://github.com/CityofToronto/bdit_data-sources.git#egg=here_api&subdirectory=here/traffic/"`

Note that because of the use of `psql` called from the Python, there is no way of specifying a password in the script. You must store your credential information in a [`.pgpass` file](https://www.postgresql.org/docs/current/static/libpq-pgpass.html)

### Using the script

```shell
Usage: here_api [OPTIONS] COMMAND [ARGS]...

  Pull data from the HERE Traffic Analytics API from --startdate to
  --enddate

  The default is to process the previous week of data, with a 1+ day delay
  (running Monday-Sunday from the following Tuesday).

Options:
  -s, --startdate TEXT
  -e, --enddate TEXT
  -d, --config PATH
  --help                Show this message and exit.

Commands:
  download  Download data from specified url to specified...
  upload    Unzip the file and pipe the data to a...
```

Using the script without one of the subcommands will perform both the [downloading](#download) of the data and the [uploading](#upload) of the data. Don't forget to specify the location of the [configuration file](sample.cfg).

#### download

```shell
Usage: here_api download [OPTIONS] DOWNLOAD_URL FILENAME

  Download data from specified url to specified filename

Options:
  --help  Show this message and exit.
```

#### upload

```shell
Usage: here_api upload [OPTIONS] DBCONFIG DATAFILE

  Unzip the file and pipe the data to a database COPY statement

Options:
  --help  Show this message and exit.
```

## Manually Load Data from Traffic Analytics Link

Data prior to 2017 was downloaded from links provided by Here. After 2017 we use the trafficanalytics portal to query the data and receive a download link for a gzipped csv. These can be downloaded directly onto the EC2 with `curl` or `wget`.

1. Check the table for the year in question exists, if not create one using the function [`sql/function_create_yearly_tables.sql`](sql/function_create_yearly_tables.sql). This will create one partitioned table per month, along with indices for the specified year under `here.ta`.
2. Load the data. It's possible to stream decompression to a PostgreSQL COPY operation without writing the uncompressed data to disk. 

Since columns cannot be reorder by COPY commands data must first be transferred to a simple staging view and then `INSERT` into `here.ta`. A full data loading command is:
```shell
# Unzip csv.gz and pipe output to psql

gunzip -c data.csv.gz | psql -h rds.ip -d bigdata -c "\COPY here.ta_view FROM STDIN WITH (FORMAT csv, HEADER TRUE);" >> bulk_load.log &

# Use curl and gunzip to directly pipe output from url to psql without downloading the file

curl -o - "{url}" | gunzip | psql -h rds.ip -d bigdata -c "\COPY here.ta_view FROM STDIN WITH (FORMAT csv, HEADER TRUE);" >> bulk_load.log &
```


