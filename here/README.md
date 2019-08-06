# HERE Data

## Traffic Data

Historical data acquired through the Traffic Analytics download portal. Data goes back to 2012-01-01 and is aggregated in 5-minute bins. In our database the data is stored in partitioned tables under `here.ta`. Have a look at the [procedure for loading new data](traffic#loading-new-data) for that including using [`data_utils`](../data_utils/), which has been extended to partition (add check constraints) and index this data.

### Data Schema

|column|type|notes|
|------|----|-----|
|link_dir|text| Unique link id |
|tx|timestamp| Timestamp of obersevation (5-min bin)|
|epoch_min|integer| Minutes of the day|
|length|integer| Link length (m)|
|mean|numeric| Observed mean speed|
|stddev|numeric| Observed speed standard deviation|
|min_spd|integer| Observed min speed|
|max_spd|integer| Observed max speed|
|confidence|integer| [10-40] degree to which observation depends on historical data (higher is better)|
|pct_x|integer| Speed at the x percentile in 5% bins|

For an exploratory description of the data check out [this notebook](https://github.com/CityofToronto/bdit_team_wiki/blob/here_evaluation/here_evaluation/Descriptive_eval.ipynb)

#### Getting link attributes

The Traffic Analytics `link_dir` is a concatenation of the `streets` layer `link_id` and a `travel direction` character (F,T). `F` and `T` represent "From" and "To" relative to each link's reference node, which is *always* the node with the lowest latitude. In the case of two nodes with equal latitude, the node with the lowest longitude is the reference node. To join `ta` data to the `here_gis.streets_att` table use the following:

```sql
JOIN here_gis.streets_att_16_1 gis ON gis.link_id = LEFT(ta.link_dir, -1)::numeric
```

#### Functional Class 5

This bucket contains a little bit of everything that doesn't fall into the other classes. Currently exclude

`"paved"  = 'Y' AND "poiaccess" =  'N' AND "ar_auto" = 'Y' AND "ar_traff" = 'Y'`

~~If we also want to exclude dead-ends add the following filter:~~  
~~`st_typ_aft NOT IN ('ACRS', 'ALY')`~~

## GIS Data

A lot of map layers provided by HERE, see the [README](gis/README.md) in the [gis](gis/) folder for more info.

## Traffic Patterns: Traffic Models

Just like the sun doesn't always shine, the streets of Toronto don't always produce vehicle probe speeds. In those cases, HERE provides us with "traffic patterns," a model for each street link by time of week. This dataset comes in a big honking `tar.gz`. Here are some handy notes for navigating and uploading this data.

`tar -lv traffic_patterns_18.tar.gz`

### Traffic Patterns: Data Model

`sql/create_traffic_patterns.sql` contains the SQL to create the tables to contain Traffic Patterns. There are 15-min and 60-min models, which have a basic wide structure of `{pattern_id, h00_00, h00_015, [...], h23_45}` where `hHH_MM` is the speed value for `pattern_id` for that time of day. Both tables share the same `pattern_ids`, which can be found in the lookup reference table `here.traffic_pattern_YY_ref` (where `YY` is the year of the model). This table is of the format `{link_pvid, travel_direction, u, m, t, w, r, f, s}` where each of those letter columns contains a `pattern_id` for that combination of `link_dir` and `day of the week` starting with sUnday.

These wide-format tables are converted to the more relational narrow format with the [sql referenced below](#converting-traffic-patterns) :point_down:.

`here.traffic_pattern_YY_ref_narrow`
|column | type | definition |
|-------|------|------------|
|link_dir |text | link direction |
|isodow |integer | ISO Day of Week|
|pattern_id |integer | id referring to the pattern for that link_dir, day of week combination|

`here.traffic_pattern_YY_spd_MM_narrow` (15 & 60 minute patterns have the same structure)
|column | type | definition |
|-------|------|------------|
|pattern_id| integer| ID for this pattern|
|trange| timerange| Time range for which this pattern applies|
|pattern_speed| integer|Speed in km/hr for that pattern_id & time range|

### Converting Traffic Patterns

[`sql/convert_traffic_patterns.sql`](sql/convert_traffic_patterns.sql) contains multiple queries to convert traffic patterns from their wide format into something that is easier to query. The queries all use [`json_build_object(VARIADIC "any")`](https://devdocs.io/postgresql~9.6/functions-json#json_build_object) to create a set of key-value pairs from the values in the columns.

For example, `json_build_object('isodow',0,'pattern_id', u)`, creates a row like `{'isodow':0, 'pattern_id': 8}`.

Next using [`json_build_array()`](https://devdocs.io/postgresql~9.6/functions-json#json_build_array) to create an array of those objects, example:

```sql
SELECT json_build_array(
        json_build_object('isodow',0,'pattern_id', u),
        json_build_object('isodow',1,'pattern_id', m))
FROM here.traffic_pattern_18_ref
LIMIT 1
-- [{'isodow':0, 'pattern_id': 8},
--  {'isodow':1, 'pattern_id': 8}]
```

And finally [`json_to_recordset(json)`](https://devdocs.io/postgresql~9.6/functions-json#json_to_recordset) converts the array into a set of defined rows, for example, the below converts each row into 7 rows of `(isodow, pattern_id)` records, linked to the original `link_dir`

```sql
SELECT link_pvid || travel_direction AS link_dir,isodow, pattern_id
INTO here.traffic_pattern_18_ref_narrow
FROM here.traffic_pattern_18_ref,
LATERAL json_to_recordset(json_build_array(
    json_build_object('isodow',0,'pattern_id', u),
--      ...
    json_build_object('isodow',6,'pattern_id', s)))
AS smth(isodow int, pattern_id int);
```
