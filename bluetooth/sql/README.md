# Bluetooth SQL

This folder contains sql queries for creating and managing the structure of the Blip data.

## [analysis](analysis)

These queries are for analysis of Bluetooth data itself and for aggregating the data to what we release for the King Street Transit Pilot Open Data Release

## [create_tables](create_tables)

Table structures for the Blip data.

## [functions](functions)

- [observations_insert_trigger()](functions/bt-trigger-observations_insert_trigger.sql): Defines a trigger on the `observations` parent table to insert new rows into the appropriate partition.
- [create_obs_unique(tablename text)](functions/create_obs_unique.sql): Add a unique constraint to the specified table so there cannot be rows with duplicate user_id, analysis_id, measured_time, measured_timestamp.
- [bluetooth.move_raw_data()](functions/move_raw_data.sql): the [blip_api script](../api) inserts into the `raw_data` table while running. At the end of uploading all that data to the database, this function is run to insert the new data into `observations` and aggregate it to 5-minute bins and insert into `aggr_5min`

## [move_data](move_data)

Various aggregation scripts. The OpenData scripts generate a format of data similar to the Public API.

- [aggr_5min_opendata_views](move_data/aggr_5min_opendata_views.sql) creates annual views of the 5-minute aggregate data to be extracted by OpenData.
- [aggr_5min_opendata_export](move_data/aggr_5min_opendata_export.sql) mimics the above for when BigData generated the OpenData files ourselves.
