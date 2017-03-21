# Traffic Segment Speeds

Data collected from a variety of traffic probes from 2007 to 2016 for major streets and arterials.

Format is:

record|tx                 | tmc     | spd | score 
------|-------------------|---------|-----|-------
ex    |2012-07-01 00:00:00|C09N32465| 32  | 10    

Where:  
 - **tx** (`timestamp`): is the timestamp of the record in UTC 
 - **tmc** (`char(9)`): is the traffic management code, the unique ID for this segment 
 - **spd** (`int`): is the speed, in mph 
 - **score** (`smallint`): is "quality" of the record {10,20,30}, with 30 being based on observed data, and 10 based on historical data.
 
You can create sample data (like [`sampledata.csv`](sampledata.csv)) using the [`python/fakedata.py`](python/fakedata.py) script. 

## Table Partitioning

Since the dataset is so massive, it's better not to have it all in one massive table. 

>Partitioning refers to splitting what is logically one large table into smaller physical pieces. Partitioning can provide several benefits

See [the documentation](https://www.postgresql.org/docs/current/static/ddl-partitioning.htm) for more details on the benefits.

Data are stored in partitioned monthly tables that inherit from  one master table which is empty. Partitioning is enabled by having a check constraint on the timestamp of each monthly table, so the query planner knows where to look for data:
```sql
CHECK (tx >= DATE '2014-01-01' AND tx < DATE '2014-01-01' + INTERVAL '1 month')
```

This works for retrieving data, and for Phase 2 data each file was already partitioned by month, so bulk loading was simple. Phase 1 data had 3-4 months in each file, so an option to automate import was to create rules on the master table on insert to assign rows to the appropriate child table. These are coded in the form:
```sql
CREATE OR REPLACE RULE inrix_raw_data_insert_201107 AS
    ON INSERT TO inrix.raw_data
    WHERE new.tx >= '2011-07-01'::date AND new.tx < ('2011-07-01'::date + '1 mon'::interval)
    DO INSTEAD
INSERT INTO inrix.raw_data201107 (tx, tmc, speed, score)
  VALUES (new.tx, new.tmc, new.speed, new.score);
```
From [the documentation](https://www.postgresql.org/docs/current/static/ddl-partitioning.html#DDL-PARTITIONING-ALTERNATIVES)
>A rule has significantly more overhead than a trigger, but the overhead is paid once per query rather than once per row, so this method might be advantageous for bulk-insert situations.

## Indexes
Indexes can be created with the [`python/create_index.py`](python/create_index.py) script. The script creates an index on:  
 - tx
 - tmc
 - score
 
Since the inrix table is partitioned into child tables, each index must be created for each individual child-table 
(see the [doc](https://www.postgresql.org/docs/current/static/ddl-partitioning.html#DDL-PARTITIONING-IMPLEMENTATION))

Since I figured the timestamp column will likely only be used with the observed data (score=30), I made that index partial by filtering it on `score=30` as per [this heroku guide](https://devcenter.heroku.com/articles/postgresql-indexes#partial-indexes). Also according to the [same guide](https://devcenter.heroku.com/articles/postgresql-indexes#multi-column-indexes), a multi-column index should only be used if the data will be consistently filtered with those two columns, otherwise multiple single-column indexes are probably a better idea.

If still updating/inserting data on a large table (like [fixing the timestamps](#timestamps)), you can `CREATE INDEX CONCURRENTLY` which doesn't fully lock up the table, but is allegedly considerably slower (same [heroku guide](https://devcenter.heroku.com/articles/postgresql-indexes#managing-and-maintaining-indexes))

## Timestamps

The timestamps are provided in UTC, but in order to make life easier for ourselves, it would be nice to convert them to local time. However we can't just subtract 5 hours to every timestamp because Daylight Savings Time still exists unfortunately. After quite a bit of searching, I found a one-liner in PostgreSQL that could do the conversion. Full example below

```sql
WITH examples AS( 
    SELECT * 
    FROM (
        VALUES (timestamp without time zone '2015-03-07 14:05:43')
             , (timestamp without time zone '2015-06-07 14:05:43')
        ) AS t (ex_time)
    )

SELECT 
ex_time AS "Example Time", 
(ex_time AT TIME ZONE 'UTC') AT TIME ZONE 'America/Toronto' AS "Converted Time"
FROM examples```

By chaining the [`AT TIME ZONE`](https://www.postgresql.org/docs/9.5/static/functions-datetime.html#FUNCTIONS-DATETIME-ZONECONVERT) command the above query converts the *timestamp without timezone* to one **with** at `UTC` and then back to a *timestamp without timezone* at `America/Toronto` time.

This is fully implemented in the python script [`update_timezone.py`](python/update_timezone.py)

Running a speed test on my local machine, the timestamp update should have been combined in the `COPY` transaction. First `COPY` to a `TEMP TABLE` then `INSERT INTO` with a `SELECT fix_timestamp(tx), ...` rather than `COPY` followed by `UPDATE`. The latter was nearly 50% slower (67 minutes vs 46).
