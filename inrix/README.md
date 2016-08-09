# Traffic Segment Speeds

Data collected from a variety of traffic probes from 2007 to 2016 for major streets and arterials.

Format is:

record|tx                 | tmc     | spd | score 
------|-------------------|---------|-----|-------
ex    |2012-07-01 00:00:00|9LX7Q6DDB| 32  | 10    

Where:  
 - **tx** (`timestamp`): is the timestamp of the record in UTC 
 - **tmc** (`char(9)`): is the traffic management code, the unique ID for this segment 
 - **spd** (`int`): is the speed, in mph 
 - **score** (`smallint`): is "quality" of the record {10,20,30}, with 30 being based on observed data, and 10 based on historical data.
 
You can create sample data (like [`sampledata.csv`](sampledata.csv)) using the [`python/fakedata.py`](python/fakedata.py) script. 

## Table Partitioning

Since the data is so massive, it's better not to have it all in one massive table.

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