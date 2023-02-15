# Table of Contents  
- [Input Parameters](#input-parameters)
    - [Geometry](#Geometry)
    - [Time Range](#time-range)
    - [Date Range](#date-range)
- [Output Parameters](#output-parameters)
- [Aggregation](#aggregation)
    - [Using congestion tables](#using-congestion-tables)
    - [Using raw speed table](#using-raw-speed-table)

## Input Parameters

- Geometry
    - The spatial extent of the study area
- Time range
    - Time periods definitions, e.g. am peak, pm peak
- Date range
    - period of time to aggregate over
    - Day of week, weekday or weekend
    - Including or excluding holidays

### Geometry

Geometries can come in a couple of different formats. Check this [guide](https://github.com/Toronto-Big-Data-Innovation-Team/bdit_data_requests/tree/master#how-to-generate-geometries-for-requests) to learn how to generate geometries. The goal is to get segments definition and their equivalent link dir lookup in the following format. 

| street_name | from_street | to_street | direction | link_dir | link_length |
| --- | --- | --- | --- | --- | --- |
| Lakeshore Blvd | Park Lawn Rd | York St | WB | 1239874T | 50 |
| Lakeshore Blvd | Park Lawn Rd | York St | WB | 1239875T | 65 |


### Time range

We typically aggregate speed data up to different periods, for example AM Peak and PM Peak. For data requests, we usually define them in a common table expression (CTE) for straight-forward modification in future data requests. For project analysis, we typically define time ranges in a table, which allows us to join directly to a table instead of repeating the same CTE in every query (and you will only need to modify one table if time ranges change!).

Example of a CTE:
```sql
-- Define time range in a CTE
WITH time_ranges(period, time_range, dow) AS (
   VALUES ('AM Long Peak'::text, '[07:00:00,10:00:00)'::timerange,'[2,5)'::int4range),
          ('AM Short Peak'::text,'[08:00:00,09:00:00)'::timerange,'[2,5)'::int4range),
          ('PM Long Peak'::text, '[15:00:00,18:00:00)'::timerange,'[2,5)'::int4range),
          ('PM Short Peak'::text,'[16:00:00,17:00:00)'::timerange,'[2,5)'::int4range)
)
```
Example of a table (from `activeto.analysis_periods`):

| analysis_period | time_period | time_range | dow_range | day_type |
| --- | --- | --- | --- | --- |
| 1 | Weekday- Daily | [00:00:00,24:00:00) | [1,5] | Weekday |
| 2 | Weekday- AM Peak | [07:00:00,10:00:00) | [1,5] | Weekday |

### Date range

Similar to time range, date ranges are defined in a `CTE` or filtered in a `WHERE` clause for data requests and in a table for project analysis. Date ranges are often project specific. For example in before and after studies, we would define different date ranges for periods such as `before`, `installation`, and `after`. In program monitoring projects, we might want to aggregate data up to a daily, weekly or monthly averages.

Example of filtering date ranges in the `WHERE` clause (not using `between`):
```sql
FROM      here.ta -- speed data table

LEFT JOIN ref.holiday holiday ON ta.tx::date = holiday.dt

WHERE     (ta.tx >= '2019-01-01 00:00:00'::timestamp without time zone AND 
          ta.tx < '2019-02-18 00:00:00'::timestamp without time zone) AND -- filter date ranges
          holiday.dt IS NULL -- excluding holiday
```

Example of defining date ranges in a `CTE`:
```sql
WITH date_period(obs_period, date_range) AS (
   VALUES ('Before'::text,'[2020-09-08,2020-10-10)'::daterange),
          ('Installation'::text,'[2019-09-16,2019-12-07)'::daterange),
          ('After'::text,'[2020-11-23,2020-12-22)'::daterange)
)
```

Example of a table (from `activeto.analysis_ranges`):

| analysis_range_id | project | analysis_range_name | date_range |
| --- | --- | --- | --- |
| 1 | KQQR | Fall 2019 | [2019-09-16,2019-12-09) |
| 2 | KQQR | Before | [2021-02-01,2021-04-05) |
| 3 | KQQR | Closure | [2021-04-05,2022-05-10) |

## Output Parameters
We typically provide average, minimum, and maximum travel time for data requests. For project analysis, output parameters are project specific, we typically estimate average travel time and travel time index for each corridor.

Common output parameters:
- average speed / travel time
- min speed / travel time
- max speed / travel time
- 85th percentile speed
- average travel time index

### Important things to note:

- Minimum sample size: Depending on the extent of the study area and the time range requests, we have to ensure we are aggregating enough data to estimate travel times, usually a minimum of a month of data.
- Harmonic mean: Harmonic mean has to be used when averaging speed, or we can average travel time or travel time index with the arithmetic mean.
- Links without data: To estimate segment level travel time when some links don't have data, we only include segments where at least 80% of links (by distance) have observations. 

## Aggregation

### Using congestion tables

Congestion summary tables are updated by an [airflow pipeline](https://github.com/CityofToronto/bdit_data-sources/blob/secret_dags/dags/congestion_refresh.py) that runs every day and aggregates 5-min bin link level data up to segment level, creating segment weekly travel time index.

When to use congestion summary tables:

Table `congestion.segments_tti_weekly` and  `congestion.segments_tti_weekly_temp` contains weekly 30-min travel time and travel time index on a segment-level for both weekday and weekend. Temporary table containing data >= 2021-04-19. Updated with new weekly data every week with an airflow DAG. Since this table aggregates data on a weekly basis, you could use it where date ranges were defined as weeks or a longer date period.

Example of `congestion.segments_tti_weekly`:

| segment_id | week | week_type | time_bin | tti_num_bins | avg_tt | avg_tti |
| --- | --- | --- | --- | --- | --- | --- |
| 1234 | 2021-02-19 | Weekday | 06:00:00 |3| 11.79 | 1.23 |
| 1224 | 2021-02-19 | Weekday | 06:30:00 | 4|13.09 | 1.34 |

**Step 1**: Calculate corridor's total length and the number of segments that make up the corridor. Knowing the total length and the total number of segments can allow us to filter corridors that do not have enough data for aggregation.

```sql
-- Calculate corridor length and number of links, as well as the sum of baseline travel time

SELECT
   corridor_id,
   sum(length) AS total_length, -- calculate the total length of each corridor
   count(segment_id) AS num_seg, -- the number of segments in each segment
   sum(tt_baseline) AS corr_baseline -- the baseline travel time of each corridor
FROM data_requests.input_table -- input table
INNER JOIN baseline_segments_tt using (segment_id) -- baseline table
GROUP BY input_table.uid 
```

**Step 2**: Aggregate segment level travel time index to the defined time period

```sql
SELECT
    segment_id,
    analysis_period,
    time_range,
    week,
    avg(tti) AS tti
FROM segment_lookup 
JOIN segments_tti_weekly  USING (segment_id)
JOIN analysis_periods USING (day_type)
WHERE week <@ date_range AND time_bin <@ time_range 
GROUP BY segment_id, analysis_period, time_range, week
```       
**Step 3**: Produces estimates of the average travel time and travel time index for each analysis period, each time period by corridors on a weekly basis, where at least 80\% of the segment (by distance) has observations at the corridor level

```sql
SELECT
   corridor_id,
   analysis_period,
   time_period,
   week,
   sum(tti * tt_baseline) / sum(tt_baseline) AS tti,
   sum(tti * tt_baseline) / sum(tt_baseline) * corr_baseline AS tt
FROM segment_tt 
GROUP BY corridor_id, analysis_period, time_period, week, corr_baseline
HAVING sum(segment_length) > (0.80 * corridor_length)::double precision
```

### Using the raw speed table `here.ta`

If the congestion tables are not suitable for your study, you can aggregate here data from the raw speed table. 

**Step 1**: Calculate corridor's total length and the number of links that make up the corridor. Knowing the total length and the total number of links can allow us to filter corridors that does not have enough data for aggregation. 

```sql
-- Calculate segment length and number of links

SELECT
   uid,
   sum(length) AS total_length, -- calculate the total length of each corridor
   count(links) AS num_seg -- the number of segments in each segment
FROM data_requests.input_table -- input table
GROUP BY input_table.uid 
```

**Step 2**: Aggregate link level travel time from 5 minutes to an hour

```sql
-- Aggregate link travel times up to an hour 
SELECT
   input_table.uid,
   input_table.link_dir,
   datetime_bin(tx, 60) AS datetime_bin, -- aggregate time from 5 min to an hour
   avg(here_length * 0.001/ mean * 3600) AS mean_tt, -- harmonic mean
FROM data_requests.input_table
JOIN here.ta USING (link_dir) -- speed data table

CROSS JOIN 	time_ranges -- CTE with define time range

WHERE 
   ( -- define date range
      tx >= '2019-01-01 00:00:00'::timestamp without time zone
      AND tx < '2019-02-18 00:00:00'::timestamp without time zone
   )
   AND tx::time without time zone <@ time_ranges.time_range 
   AND date_part('isodow'::text, a.tx)::integer <@ time_ranges.dow

GROUP BY input_table.uid, input_table.link_dir, datetime_bin, input_table.length, period
```

**Step 3**: Aggregate link level hourly travel time up to corridor level, where at least 80\% of the corridor (by distance) has observations

```sql
-- Aggregate link level hourly travel time to corridor level
SELECT
   uid,
   link_hourly.datetime_bin,
   corridor_detail.total_length / (sum(link_hourly.here_length) / sum(link_hourly.mean_tt)) AS corr_tt
FROM link_hourly
INNER JOIN corridor_detail USING (uid)
GROUP BY link_hourly.datetime_bin, uid, corridor_detail.total_length, period
HAVING sum(link_hourly.here_length) >= (total_length * 0.8) -- where at least 80% of links have data
```

**Step 4**: Aggregate corridor level hourly travel time up to each defined time periods for each day

```sql
-- Aggregate corridor level hourly travel time to time periods	
SELECT
   uid,
   period,
   avg(corr_tt) AS corr_mean_tt
FROM corridor_hourly
GROUP BY period, uid, total_length
```

**Step 5**: Produces estimates of the minimum, average and maximum travel time for each time period by corridors

```sql
SELECT
   uid,
   period,
   min(corr_mean_tt) AS min_tt,
   avg(corr_mean_tt) AS mean_tt,
   max(corr_mean_tt) AS max_tt
FROM corridor_agg
GROUP BY uid, period
```
