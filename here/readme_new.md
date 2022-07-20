# Table of Contents

- [Basic Info](#basic-info)
    - [Keywords](#keywords)
    - [Title of Dataset](#title-of-dataset)
    - [Description: What is HERE data?](#description-what-is-here-data)
    - [Uses for HERE data](#uses-for-here-data)
    - [Types of HERE data products](#types-of-here-data-products)
    - [HERE Data: dates and time periods](#here-data:-dates-and-time-periods)
- [HERE data at a glance](#here-data-at-a-glance)
- [Detailed dataset overview](#detailed-dataset-overview)
    - [Traffic data (aka the "ta" for "traffic analytics" table)]
        - [Data Schema]
            - [Getting link attributes]
            - [Functional Class 5]
    - [GIS Data]
    - [Traffic Patterns: Traffic Models]
    - [Routing with Traffic Data]
    - [Aggregating Traffic Data]
- [Links to HERE data documentation (in one place)]
- [But I still have questions!!!]

# Basic Info <a name = basic-info></a>

## Keywords
travel time, congestion, buffer index, probe data, speed

## Title of Dataset:
HERE data

## Description: What is HERE data?

HERE data is travel time data provided by HERE Technologies from a mix of vehicle probes. We have a daily [automated airflow pipeline](https://github.com/CityofToronto/bdit_data-sources/blob/master/dags/pull_here.py) that pulls 5-min aggregated speed data for each link in the city from the here API. For streets classified collectors and above, we aggregate up to segments using the [congestion network](https://github.com/CityofToronto/bdit_congestion/tree/grid/congestion_grid) and produce [summary tables](https://github.com/CityofToronto/bdit_congestion/blob/data_aggregation/congestion_data_aggregation/sql/generate_segments_tti_weekly.sql) with indices such as Travel Time Index and Buffer Index. 

*Travel Time Index: is the ratio of the average travel time and free-flow speeds. For example, a TTI of 1.3 indicates a 20-minute free-flow trip requires 26 minutes.*

*Buffer Index: indicates the additional time for unexpected delays that commuters should consider along with average travel time. For example, if BI and average travel time are 20% and 10 minutes, then the buffer time would be 2 minutes. Since it is calculated by 95th percentile travel time, it represents almost all worst-case delay scenarios and assures travelers to be on-time 95 percent of all trips.*

This is the coverage of here links in the city of Toronto. (from `here_gis.streets_21_1`)
![image](https://user-images.githubusercontent.com/46324452/149184544-bbff447b-bba7-4585-aebf-d9fc65d21998.png)

This is the coverage of the congestion network. 
![image](https://user-images.githubusercontent.com/46324452/149438775-20360279-10ef-4963-8d6a-188e934bb0c2.png)

## Uses for HERE data:
We use HERE data to:
- Calculate travel time for aggregated time periods of at least three weeks for any two locations in the City (which can be used to measure the effects of infrastructure improvements like bike lanes);
- Examine changes to the Travel Time Index over time (to see the effects of road closures or Provincially mandated lock downs); and,
- Determine the 85th percentile speed (to inform speed limit or enforcement initiatives).

## Types of HERE data products
HERE Technologies provides us with a few different datasets, such as:
- traffic analytics tables containing probe data for all Toronto streets
- traffic patterns tables containing aggregated speed information for all Toronto streets
- street attribute tables showing speed limits, street names and one way info for Toronto's streets
- many GIS layers of streets, intersections, points of interest, trails, rail corridors and water features.

## HERE Data: dates and time periods
As stated above, the traffic analytics table (here.ta) is updated daily. 

Other products are generally updated quarterly, though this schedule has been somewaht disrupted due to the pandemic.

There are also aggregated tables or views specific to certain modes (like trucks or cyclists) or time periods (like weekends or night-time).

We have HERE data going back to 2012, but there sure weren't as many people walking around with "probes" in their pockets back then! Therefore, 2014 is generally seen as the first year for which travel times can be reasonably relied upon.

Travel times are calculated for time periods that are at least three weeks or longer. Shorter time periods will not generate enough accurate probe data.

# HERE data at a glance

The Question       | The Answer     |
:----------------- | :---------------
What is the HERE dataset used for? | To calculate travel times and monitor congestion, mostly
Where is the HERE dataset from? | HERE Technologies, via an agreement with the Ontario Ministry of Transportation (MTO)
Is it available on Open Data? | No
What area does the HERE dataset represent? | All of Toronto; the co-ordinate system is EPSG: 4326
Where is it stored? | On an internal postgres database called bigdata; in several schema (here, here_analysis, here_eval and here_gis)
How often is it updated? | Probe data are updated every day; reference files are usually updated quarterly
How far back does it go? | To 2012, but 2014 data are much more accurate
What are the limitations? | Travel times are generally calculated for time periods lasting three or more weeks
What can I get? | Custom aggregations
Are raw data available? | No
How do I cite HERE data? | ???
Who can I contact about HERE data? | Email us at transportationdata@toronto.ca

# Detailed dataset overview <a name="detailed-dataset-overview"></a>

## Traffic data (aka the "ta" for "traffic analytics" table)

Historical data acquired through the Traffic Analytics download portal. Data goes back to 2012-01-01 and is aggregated in 5-minute bins. In our database the data is stored in partitioned tables under `here.ta`. Have a look at the [procedure for loading new data](traffic/README.md) for that including using [`data_utils`](../data_utils/), which has been extended to partition, check constraints and index this data.

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

For an exploratory description of coverage (or how much probe data there is for our roads) check out [this notebook](https://github.com/CityofToronto/bdit_team_wiki/blob/here_evaluation/here_evaluation/Descriptive_eval.ipynb).

#### Getting link attributes

The Traffic Analytics `link_dir` is a concatenation of the `streets` layer `link_id` and a `travel direction` character (F,T). `F` and `T` represent "From" and "To" relative to each link's reference node, which is *always* the node with the lowest latitude. In the case of two nodes with equal latitude, the node with the lowest longitude is the reference node. To join `ta` data to the `here_gis.streets_att` table use the following:

```sql
JOIN here_gis.streets_att_16_1 gis ON gis.link_id = LEFT(ta.link_dir, -1)::numeric
```

#### Functional Class 5

This bucket contains a little bit of everything that doesn't fall into the other classes. Currently exclude

`"paved"  = 'Y' AND "poiaccess" =  'N' AND "ar_auto" = 'Y' AND "ar_traff" = 'Y'`

## GIS Data

A lot of map layers provided by HERE, see the [README](gis/README.md) in the [gis](gis/) folder for more info.

## Traffic Patterns: Traffic Models

Just like the sun doesn't always shine, the streets of Toronto don't always produce vehicle probe speeds. In those cases, HERE provides us with traffic patterns, a model for each street link by time of week.

### Traffic Patterns: Data Model

`sql/create_traffic_patterns.sql` contains the SQL to create the tables that contain Traffic Patterns. There are 15-min and 60-min models, which have a basic wide structure of `{pattern_id, h00_00, h00_015, [...], h23_45}` where `hHH_MM` is the speed value for `pattern_id` for that time of day. Both the 15-min and 60-min tables share the same `pattern_ids`, which can be found in the lookup reference table `here.traffic_pattern_YY_ref` (where `YY` is the year of the model). This table is of the format `{link_pvid, travel_direction, u, m, t, w, r, f, s}` where each of those letter columns contains a `pattern_id` for that combination of `link_dir` and `day of the week` starting with sUnday.

These wide-format tables are converted to a narrow format which makes relating traffic patterns to other datasets much easier.

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

Check out this [documentation](here/here_loading.md) to see how the "traffic patterns" tables are loaded and transformed.

## Routing with Traffic Data

One use of historical traffic data is the ability to route a vehicle from an
arbitrary point to another arbitrary point using traffic data **at that point
in time**. Since our data is already in a database, this can be accomplished
using the [`pgRouting`](http://pgrouting.org/) PostgreSQL extension. It is
necessary to have [traffic patterns](#traffic-patterns-traffic-models) loaded
to fill in gaps in traffic data in time.

The following views prepare the HERE data for routing (code found
[here](traffic/sql/create_here_routing.sql)):

- `here.routing_nodes`: a view of all intersections derived from the `z_levels`
  gis layer.
- `here.routing_streets_18_3`: The geography of streets is provided as
  centerlines, but traffic is provided directionally. This view creates
  directional links for each permitted travel direction on a navigable street
  with a `geom` drawn in the direction of travel.

The function
[`here.get_network_for_tx()`](traffic/sql/function_routing_network.sql)
generates a network routeable in pgrouting by pulling traffic data for the
5-minute timestamp starting at `tx` and merging that with traffic patterns for
that weekday and time of day to
fill in missing data. It returns the following columns:

|column | type | desc|
|-------|------|-----|
id     | int | unique numeric id for the `link_dir`
source     | int | id of the source node
target     | int | id of the target node
cost     | int | "cost" for this link, in travel time seconds based on the traffic speed

It can be used in the
[`pgr_dijkstra`](http://docs.pgrouting.org/latest/en/pgr_dijkstra.html) family
of functions using SQL like the following, replaced `TX` with the appropriate timestamp:

```sql
SELECT * FROM pgr_dijkstra('SELECT * FROM here.get_network_for_tx(TX)', start_vertex_id, end_vertex_id)
```

## Aggregating Traffic Data

HERE Traffic time data is at a link and 5-min resolution but, for data requests and projects we typically aggregate them up to a segment or over a certain time period. Check out this [documentation](https://github.com/CityofToronto/bdit_data-sources/blob/master/here/here_aggregation.md) to learn more about aggregating here data.

# All the links to HERE data documentation (in one place)
...by order of appearance in this readme...
* [Procedure for loading new data](traffic/README.md) 
* [HERE GIS datasets](gis/README.md)
* [Aggregating HERE data](here/here_aggregation.md)

# But I still have questions!!!
Awesome! We love talking about data. Further inquiries about HERE data should be sent to transportationdata@toronto.ca.  