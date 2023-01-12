# Traffic Analytics Data

- [here_api.py](#here_apipy)
  - [Setup](#setup)
  - [Using the script](#using-the-script)
    - [download](#download)
    - [upload](#upload)
- [Loading New Data (Old Method)](#loading-new-data-old-method)

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

## Loading New Data (Old Method)

Data prior to 2017 was downloaded from links provided by Here. After 2017 we use the trafficanalytics portal to query the data and receive a download link for a gzipped csv. These can be downloaded directly onto the EC2 with `curl` or `wget`.

1. Check the table for the months in question exists, if not create one using the relevant part of the loop in the [`sql/create_tables.sql`](sql/create_tables.sql) script. This will create a partitioned table for the specified month and also create a rule to insert data into this partition.
2. Load the data. It's possible to stream decompression to a PostgreSQL COPY operation without writing the uncompressed data to disk, so [do that](https://github.com/CityofToronto/bdit_team_wiki/wiki/PostgreSQL#copying-from-compressed-files). Since [rules aren't triggered](https://github.com/CityofToronto/bdit_team_wiki/wiki/PostgreSQL#table-partitioning) by COPY commands data must first be transferred to a staging table and then `INSERT`ed into `here.ta`. A full data loading command is:
```shell
gunzip -c data.csv.gz | psql -h rds.ip -d bigdata -c "\COPY here.ta_staging FROM STDIN WITH (FORMAT csv, HEADER TRUE); INSERT INTO here.ta SELECT * FROM here.ta_staging; TRUNCATE here.ta_staging;" >> bulk_load.log &
```

3. Add check constraints. [`data_util`](../../data_util) works with HERE data. So a command for one table would be `./data_util.py -p -d db.cfg -y 201701 201701 -s here -t ta_`
4. Add indexes using `data_util`. Relevant command is `./data_util.py -i -d db.cfg -y 201701 201703 --idx link_id --idx timestamp --schema here --tablename ta_`
