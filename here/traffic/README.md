# Traffic Analytics Data

## Loading New Data

Data prior to 2017 was downloaded from links provided by Here. After 2017 we use the trafficanalytics portal to query the data and receive a download link for a gzipped csv. These can be downloaded directly onto the EC2 with `curl` or `wget`. 

1. Check the table for the months in question exists, if not create one using the relevant part of the loop in the [`sql/create_tables.sql`](sql/create_tables.sql) script. This will create a partitioned table for the specified month and also create a rule to insert data into this partition.
2. Load the data. It's possible to stream decompression to a PostgreSQL COPY operation without writing the uncompressed data to disk, so [do that](https://github.com/CityofToronto/bdit_team_wiki/wiki/PostgreSQL#copying-from-compressed-files). Since [rules aren't triggered](https://github.com/CityofToronto/bdit_team_wiki/wiki/PostgreSQL#table-partitioning) by COPY commands data must first be transferred to a staging table and then `INSERT`ed into `here.ta`. A full data loading command is:
```shell
gunzip -c data.csv.gz | psql -h rds.ip -d bigdata -c "\COPY here.ta_staging FROM STDIN WITH (FORMAT csv, HEADER TRUE); INSERT INTO here.ta SELECT * FROM here.ta_staging; TRUNCATE here.ta_staging;" >> bulk_load.log &
```

3. Add check constraints. [`data_util`](../../data_util) works with HERE data. So a command for one table would be `./data_util.py -p -d db.cfg -y 201701 201701 -s here -t ta_`
4. Add indexes using `data_util`. Relevant command is `./data_util.py -i -d db.cfg -y 201701 201703 --idx link_id --idx timestamp --schema here --tablename ta_` 