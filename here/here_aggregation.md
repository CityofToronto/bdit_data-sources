# What is HERE data?

We get speed data for all streets in the City of Toronto from HERE Technologies which summarizes GPS traces of connected cars, trucks, and other devices. We have a daily automated airflow pipeline that pulls 5 minutes aggregated speed data for each link in the city from the here API. 

This is the coverage of here links in the city of Toronto. (from `here_gis.streets_21_1`)

# Data Requests

We often get data requests from other units asking for speed data on certain streets over a certain amount of time. For example before and after studies for bikelane installations, and reoccurring requests such as school zone prioritization, and traffic signal optimization. 

Before aggregating the data, we need to figure out the input and output parameters first:

**Input Parameters**

- Geometry
    - The spatial extent of the study area
- Time range
    - Time periods definitions, e.g. am peak, pm peak
- Date range
    - Over what period of time?
    - Day of week, weekday or Weekend
    - Including or excluding holidays

**Output Parameters**

- average speed / travel time
- min speed / travel time
- max speed / travel time
- 85th percentile speed

## Geometry

Geometries can come in a couple of different formats. The goal is to get segments definition and their equivalent link dir lookup in the following format. 

| street_name | from_street | to_street | direction | link_dir | link_length |
| --- | --- | --- | --- | --- | --- |
| Lakeshore Blvd | Park Lawn Rd | York St | WB | 1239874T | 50 |
| Lakeshore Blvd | Park Lawn Rd | York St | WB | 1239875T | 65 |

### Travel time request app

The travel time data request app (only accessible with VPN) was created by U of T students as a part of their computer science project. It allows users to draw segments using the here routing network, and download the drawn segments as geojson. There are a few issues with the returned geojson (an extra `[]` brackets, and some formatting issues around multi linestrings) 

Steps to format geojson from the app:

1) Open the geojson on a text editor and get rid of the outer most square brackets `[]`

2) Inspect the geojson on [geojson.io](http://geojson.io) or QGIS 

3) QC, import the geojson in QGIS and make sure the segments were drawn correctly

- Check the direction of the segment
- Definition of the segment

4) Import the geojson layer in our postgres database, in the `data_requests` schema. The easiest way is to drag the geojson file opened in QGIS to the database. 

5) In the postgres table, clean and organize segments into the following columns:

| street_name | from_street | to_street | direction | link_dir | link_length |
| --- | --- | --- | --- | --- | --- |
| Lakeshore Blvd | Park Lawn Rd | York St | WB | 1239874T | 30 |

[Example](https://github.com/Toronto-Big-Data-Innovation-Team/bdit_data_requests/blob/traffic_op_20210107/input_manipulation%20/here_routing/traffic_signal_optimization/i0113-clean_data.sql) cleaning sqls. 

### PX numbers (Traffic Signals)

Another format geometries are defined are from and to PX numbers, which are traffic signal IDs. For example:

| px_start | px_end | street | from_street | to_street |
| --- | --- | --- | --- | --- |
| PX102 | PX150 | University Ave | Wellington  | Adelaide |
| PX150 | PX156 | University Ave | Adelaide | Richmond |

In this case, we will need to “draw” the segments using [pgRouting](!http://pgrouting.org/), a PostgreSQL extension in pgadmin. There is a function `here_gis.get_links_btwn_px_21_1(px_start, px_end)` in our database which takes in two input: start px number, end px number and returns the link_dirs that make up the segment between two pxs. It first find the closest here node from the input px geometry using nearest neighbour, and then route the here nodes using `pgr_dijkstra` which finds the shortest path between two nodes. 

Steps to draw segments from PX:

1) Organize and create an input table with px number and street name to the `data_request` schema

2) Run function `here_gis.get_links_btwn_px_21_1(px_start, px_end)` 

3) QC and organize results into the following columns:

| street_name | from_street | to_street | direction | link_dir | link_length | px_start | px_end |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Lakeshore Blvd | Park Lawn Rd | York St | WB | 1239874T | 30 | PX123 | PX124 |

### Text Description

Sometimes, we don’t get anything other than a simple text description. 

| street_name | from_street | to_street |
| --- | --- | --- |
| Lakeshore Blvd | Park Lawn Rd | York St |

In this case, we can either go to the travel time request app and draw the segment there, or route them with a function. 

Steps to draw segments from text description:

1) Find the start and end nodes of each segment. The here nodes table is in `here.routing_nodes_21_1` . The table is quite large but you can still load it in QGIS or you can find them on the app. 

2) Run function `here_gis.get_links_btwn_nodes_21_1(start_node,end_node)` 

3) QC and organize results into the following columns:

| street_name | from_street | to_street | direction | link_dir | link_length |
| --- | --- | --- | --- | --- | --- |
| Lakeshore Blvd | Park Lawn Rd | York St | WB | 1239874T | 30 |

Things to note when QCing:

- Check the direction of the geometry and see if it matches the description. You can check them using the function `gis.direction_from_line(geom)` or 👀 on QGIS.
- Be extra careful with streets with medians. A lot of the times HERE draws streets with medians in two separate lines. Make sure you have the correct node for those streets or routing might act bit a wonky.

## Time and Date range

First identify the time and date range from the request, often times it is asking for data to aggregate up to am and pm peak periods. We usually define them in the query using a `CTE`, for straight forward modification later and reusability of the query:

```sql
-- Define time range
WITH time_ranges(period, time_range, dow) AS (
	VALUES ('AM Long Peak'::text, '[07:00:00,10:00:00)'::timerange,'[2,5)'::int4range), 
		     ('AM Short Peak'::text,'[08:00:00,09:00:00)'::timerange,'[2,5)'::int4range), 
		     ('PM Long Peak'::text, '[15:00:00,18:00:00)'::timerange,'[2,5)'::int4range), 
		     ('PM Short Peak'::text,'[16:00:00,17:00:00)'::timerange,'[2,5)'::int4range))
```

For date range, we usually filter them in the where clause (not using `between`)

```sql
FROM        here.ta -- speed data table
CROSS JOIN  time_ranges -- CTE with define time range
LEFT JOIN   ref.holiday holiday ON tx::date = holiday.dt
WHERE       (tx >= '2019-01-01 00:00:00'::timestamp without time zone AND 
	         tx < '2019-02-18 00:00:00'::timestamp without time zone) -- define date range
		    tx::time without time zone <@ time_ranges.time_range AND 
		    date_part('isodow'::text, a.tx)::integer <@ time_ranges.dow AND 
		    holiday.dt IS NULL -- excluding holiday
```

## Aggregation

Step 1: Define time range

```sql
-- Define time range
WITH time_ranges(period, time_range, dow) AS (
	VALUES ('AM Long Peak'::text, '[07:00:00,10:00:00)'::timerange,'[2,5)'::int4range), 
		     ('AM Short Peak'::text,'[08:00:00,09:00:00)'::timerange,'[2,5)'::int4range), 
		     ('PM Long Peak'::text, '[15:00:00,18:00:00)'::timerange,'[2,5)'::int4range), 
		     ('PM Short Peak'::text,'[16:00:00,17:00:00)'::timerange,'[2,5)'::int4range))
```

Step 2: Calculate segment’s total length and the number of links that make up the segment. This is an important step to make sure we have enough link dirs that has data to estimate the travel time of a segment. 

```sql
-- Calculate segment length and number of links
, seg AS (
	SELECT 		i0168_input_table.uid,
				sum(i0168_input_table.length) AS total_length, -- calculate the total length of each segment
				count(i0168_input_table.link_dir) AS num_seg -- the number of link dir in each segment
	FROM 		data_requests.i0168_input_table -- input table
	GROUP BY 	i0168_input_table.uid )
```

Step 3: Aggregate link level travel time from 5 minutes to an hour

```sql
-- Aggregate link travel times up to an hour 
, link_hourly AS (
	SELECT 		input_table.uid,
				input_table.link_dir,
				period,
				input_table.length AS here_length,
				datetime_bin(a_1.tx, 60) AS datetime_bin, -- aggregate time from 5 min to an hour
				avg(input_table.length * 0.001/ a_1.mean * 3600) AS mean_tt, -- harmonic mean

	FROM 		data_requests.i0168_input_table input_table
	JOIN 		here.ta USING (link_dir) -- speed data table
	CROSS JOIN 	time_ranges -- CTE with define time range
	LEFT JOIN 	ref.holiday holiday ON tx::date = holiday.dt
	WHERE 		(tx >= '2019-01-01 00:00:00'::timestamp without time zone AND 
				 	tx < '2019-02-18 00:00:00'::timestamp without time zone) -- define date range
			    tx::time without time zone <@ time_ranges.time_range AND 
			    date_part('isodow'::text, a.tx)::integer <@ time_ranges.dow AND 
			    holiday.dt IS NULL -- excluding holiday
	GROUP BY 	input_table.uid, input_table.link_dir, datetime_bin, input_table.length, period)
```

Step 4: Aggregate link level hourly travel time up to corridor level (80% length)

```sql
-- Aggregate link level hourly travel time to corridor level
, corridor_hourly AS (
	SELECT 		b.uid,
			    link_hourly.datetime_bin,
				period, 
			    b.total_length / (sum(link_hourly.here_length) / sum(link_hourly.mean_tt)) AS corr_tt, 
				b.total_length
    FROM 		link_hourly
	JOIN 		seg b USING (uid)
	GROUP BY 	link_hourly.datetime_bin, b.uid, b.total_length, b.num_seg, period
	HAVING 		sum(link_hourly.here_length) >= (b.total_length * 0.8) -- where at least 80% of links have data
	ORDER BY 	b.uid, b.num_seg)
```

Step 5: Aggregate corridor level hourly travel time up to each defined time periods for each day

```sql
-- Aggregate corridor level hourly travel time to time periods	
, corridor_agg AS (
   SELECT 		corridor_hourly.uid,
				corridor_hourly.period,
            	avg(corridor_hourly.corr_tt) AS corr_mean_tt,
            	corridor_hourly.total_length
   FROM 		corridor_hourly
   GROUP BY  period, uid, total_length),
```

Step 6: Calculate output parameters

```sql
SELECT 	    uid,
            street,
			from_street,
			to_street,
			direction,
			period,
            min(corridor_agg.corr_mean_tt) AS min_tt,
            avg(corridor_agg.corr_mean_tt) AS mean_tt,
            max(corridor_agg.corr_mean_tt) AS max_tt,
            corridor_agg.total_length / max(corridor_agg.corr_mean_tt) * (3600.0 / 1000.0)::double precision AS min_spd,
            corridor_agg.total_length / avg(corridor_agg.corr_mean_tt) * (3600.0 / 1000.0)::double precision AS mean_spd,
            corridor_agg.total_length / min(corridor_agg.corr_mean_tt) * (3600.0 / 1000.0)::double precision AS max_spd,
            corridor_agg.total_length
FROM 	    corridor_agg
INNER JOIN  data_requests.i0168_input_table USING (uid) -- join to input table to get street info
GROUP BY    uid, street, from_street, to_street, direction, period, total_length
```

## Important things to note:

- Minimum sample size: Depending on the extent of the study area and the time range requests, we have to ensure we are aggregating enough data to estimate travel times.
- Harmonic mean: harmonic mean provides the correct average when averaging speed, using arthmetic mean is not correct.
- Links without data: To estimate corridor level travel time when some links are missing data, we only include corridors where at least 80% of the length has observations. 


# Project Analysis

The steps to aggregate here data in project analysis are similar to data requests. They are slightly different in terms of the way we structure and organize data, as well as the base network used. 

For example, bigger projects such as ActiveTO and RapidTO has their own schema `activeto`, and `rapidto`. SQLs for smaller projects reside in schema `data_analysis`. Time range, date range, as well geometry are usually defined in separate tables or views for easier modification and reusability. 

### Define time period in a view or a table

Example from `activeto.analysis_periods`

| analysis_period | time_period | time_range | dow_range | day_type |
| --- | --- | --- | --- | --- |
| 1 | Weekday- Daily | [00:00:00,24:00:00) | [1,5] | Weekday |
| 2 | Weekday- AM Peak | [07:00:00,10:00:00) | [1,5] | Weekday |

### Define date range

Example from `activeto.analysis_ranges` 

| analysis_range_id | project | analysis_range_name | date_range |
| --- | --- | --- | --- |
| 1 | KQQR | Fall 2019 | [2019-09-16,2019-12-09) |
| 2 | KQQR | Before | [2021-02-01,2021-04-05) |
| 3 | KQQR | Closure | [2021-04-05,2022-05-10) |

### Define segments

Defining segments for projects are often different than data requests, since a lot of the time we use congestion network instead of here network. Using the congestion network allows us to create other monitoring indices  such as Travel Time Index (TTI) and Buffer Index (BI).

Example table structure:

| report_id | corr_name | street_name | from_street | to_street | direction | segment_id |
| --- | --- | --- | --- | --- | --- | --- |
| 48	 | Eglinton | Eglinton Ave/ Kingston Rd | Guildwood Pkwy | Markham Rd | Westbound | 9646 |
| 48	 | Eglinton | Eglinton Ave/ Kingston Rd | Guildwood Pkwy | Markham Rd | Westbound | 22125 |

## Steps to aggregate here speed data:

### Using the raw speed table `here.ta`

(this example is from rapidto)

Step 1: Produce an estimate of the average travel time for each 30 minute bin for each individual link (`link_dir`)

```sql
/*
speed_links: Produces estimates of the average travel time for each 30-minute bin for each individual link (link_dir)
*/
, speed_links AS (
    SELECT report_id,
        segment_id,
        ta.link_dir,
        ta.length AS link_length,
	datetime_bin(ta.tx, 30) AS datetime_bin,
        harmean(ta.mean) AS spd_avg,
        count(DISTINCT ta.tx) AS count
    FROM here.ta
    JOIN congestion.segment_links_v6_21_1 USING (link_dir)
    JOIN rapidto.report_segments USING (segment_id)
    LEFT JOIN ref.holiday hol ON hol.dt = ta.tx::date
    CROSS JOIN properties p
    WHERE hol.dt IS NULL AND ta.tx >= '2019-09-16'::date
    AND report_id = ANY (p.id_array) 
    GROUP BY report_id, segment_id, ta.link_dir, datetime_bin, ta.length
)
```

Step 2: Aggregate average link_dir travel time up to individual segments (`segment_id`) using the lookup table `congestion.segment_links_v6_21_1`, where at least 80% of the segment (by distance) has observations at the link (`link_dir`) level

```sql
/*
tt_30: Produces estimates of the average travel time for each 30-minute bin for each individual segment (segment_id), where at least 80% of the segment (by distance) has observations at the link (link_dir) level
*/
, tt_30 AS (
    SELECT report_id,
        segment_id,
        datetime_bin,
        CASE
            WHEN sum(link_length) >= (0.8 * b.length) THEN sum(link_length / spd_avg * 3.6) * b.length / sum(link_length)
        ELSE NULL
        END AS segment_tt_avg
    FROM speed_links
    JOIN congestion.segments_v6 b USING (segment_id)
    WHERE date_part('isodow', datetime_bin)::integer <@ '[1,6)'::int4range
    GROUP BY report_id, segment_id, datetime_bin, b.length
    ORDER BY report_id, segment_id, datetime_bin
)
```

Step 3: Aggregate segments level travel times up to a month for each 30-minute bin

```sql
/*
monthly_tt: Produces estimates of the average travel time for each month for each 30-minute bin by segment (segment_id)
*/
, monthly_tt AS (
    SELECT a.report_id,
        a.segment_id,
        a.datetime_bin::time without time zone AS time_bin,
        count(a.datetime_bin) AS num_bins,
        obs_period, --date_period, --obs_period,
        b.seg_length,
        avg(a.segment_tt_avg) AS avg_tt,
        CASE
            WHEN highway.segment_id IS NOT NULL THEN b.tt_baseline_10pct_corr
        ELSE b.tt_baseline_25pct_corr
        END AS baseline_tt
    FROM tt_30 a
    LEFT JOIN congestion.tt_segments_baseline_v5_2019_af b USING (segment_id)
    LEFT JOIN congestion.highway_segments_v6 highway USING (segment_id)
    INNER JOIN rapidto.analysis_ranges ON a.datetime_bin::date <@ date_range
    WHERE obs_period != 'Lockdown 2'
    GROUP BY a.report_id, a.segment_id, obs_period, highway.segment_id,
      		(a.datetime_bin::time without time zone), b.seg_length, 
		b.tt_baseline_10pct_corr, b.tt_baseline_25pct_corr
)
```

Step 4: Produce an estimate of the average travel times index (tti) for each month for each peak period (defined in an cte) by segment

```sql
/*
segment_tt: Produces estimates of the average travel time index for each month for each 30-minute bin by segment (segment_id)
*/
, segment_tt AS (
    SELECT tti.report_id,
        tti.segment_id,
        tti.seg_length AS segment_length,
        tti.obs_period,
	peak_period.tod_period AS period,
        tti.time_bin,
        tti.num_bins AS tti_num_bins,
        tti.avg_tt,
        tti.avg_tt / tti.baseline_tt AS tti,
        tti.baseline_tt
    FROM monthly_tt tti
    INNER JOIN rapidto.analysis_periods peak_period ON tti.time_bin <@ peak_period.time_range
    WHERE peak_period.tod_period IN ('AM Peak Period', 'PM Peak Period', 
				   'AM Peak 6h-7h', 'AM Peak 7h-8h', 
 				   'AM Peak 8h-9h', 'PM Peak Period', 
                                   'PM Peak 15h-16h', 'PM Peak 16h-17h', 
				   'PM Peak 17h-18h', 'PM Peak 18h-19h')
)
```

Step 5: Produce an estimate of the average travel time index (tti) and average travel time for each periods, 

```sql
/*
period_tt: Produces estimates of the average travel time index for each month for each peak period by segment (segment_id)
*/
, period_tt AS (
    SELECT report_id,
        segment_id,
        obs_period,
        period,
        avg(tti) AS tti,
        CASE
            WHEN avg(tti) IS NOT NULL THEN segment_length
            ELSE 0
        END AS segment_length,
        baseline_tt,
        sum(
            CASE
                WHEN tti IS NOT NULL THEN 1
                ELSE 0
            END) AS num_30obs
    FROM segment_tt
    GROUP BY report_id, segment_id, obs_period,
		period, segment_length, baseline_tt
)
```

Step 6: Produce an estimate of the average travel time index (tti) and average travel time for each date range by report corridors (report_id), where at least 80% of the report corridor (by distance) has observations at the segment (segment_id) level

```sql
/*
final_all_segments: Produces estimates of the average travel time index and average travel time for each period, each date range by report corridors (report_id),
where at least 80% of the report corridor (by distance) has observations at the segment (segment_id) level
*/
, final_all_segments AS (
	SELECT report_id,
		num_segments.street_name,
		num_segments.from_street,
		num_segments.to_street,
		num_segments.dir,
		obs_period AS date_period,
		period,
		SUM(tti * baseline_tt) / SUM(baseline_tt) * report_baselines.report_tt_baseline / 60 AS tt
	FROM period_tt
	JOIN num_segments USING (report_id)
	JOIN report_baselines USING (report_id)
	GROUP BY report_id, num_segments.street_name, num_segments.from_street,
		num_segments.to_street, num_segments.dir,
		obs_period, period, num_segments.report_length,
		report_baselines.report_tt_baseline
	HAVING (num_segments.report_length * 0.8) < sum(segment_length)
)
```

### Using congestion tables

Congestions summary tables are updated by an airflow pipeline that runs every day and aggregates 5 min bin link level data up to segment level, creating segment weekly travel time index and citywide travel time index. 

Aggregating with congestion tables are simpler than aggregating directly from the raw speed table. We can omit step 1 to 3, and start from step 4. Select directly from summary tables such as weekly segment level tti from `congestion.segments_weekly_tti`. Follow step 5 and 6 and convert tti to travel times using the baseline travel time for each segments. 

For example:
```
SELECT 	corridor_id,
		analysis_period,
		time_period,
		week,
		SUM(A.tti * B.tt_baseline) / SUM(B.tt_baseline) AS tti, 
		SUM(A.tti * B.tt_baseline) / SUM(B.tt_baseline) * C.tt_baseline AS tt 

FROM 		segment_tt A
INNER JOIN	segment_baseline B USING (segment_id)
INNER JOIN	corridor_baseline C USING (corridor_id)
INNER JOIN	congestion.segments_v5 D USING (segment_id)
INNER JOIN	activeto.kqqr_here_corridors E USING (corridor_id)
```
