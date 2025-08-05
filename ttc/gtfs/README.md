- [GTFS data](#gtfs-data)
  - [Archive](#archive)
    - [Relationships of GTFS tables and CIS tablespace](#relationships-of-gtfs-tables-and-cis-tablespace)

# GTFS data

The [General Transit Feed Specification
](http://gtfs.org/) (GTFS) is a static, human-and-machine-readable transit schedule, originally intended to provide transit routing for Google Maps. Because our analyses are historical, we need to examine multiple schedules. These schedules are stored together in tables in the `gtfs` schema. To distinguish between schedules a `feed_id` column was added to every table. This column may not be in chronological order. Look at `feed_info` to know when a schedule was inserted, and `calendar` to see the range of dates for which the feed was valid.

An Airflow DAG `gtfs_pull` checks [Toronto Open data](https://open.toronto.ca/dataset/ttc-routes-and-schedules/) for new schedules daily and inserts them into bigdata `gtfs` schema with `feed_info.insert_date` as the `last_refreshed` attribute from Open Data.

## Archive

### Relationships of GTFS tables and CIS tablespace

The entity relationship diagram (ERD) indicates the common columns in the schema `gtfs` in the Big Data Innovation Team's own PostgreSQL database, and how schema `gtfs` and table `cis_2017` in schema `ttc` share the same value of information.

!['gtfs&cis_relationship'](archive/img/gtfs_cis.png)
