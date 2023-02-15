# HERE Data

- [Basic Info](#basic-info)
    - [Description: What is HERE data?](#description-what-is-here-data)
    - [Uses for HERE data](#uses-for-here-data)
    - [Types of HERE data products](#types-of-here-data-products)
    - [HERE Data: dates and time periods](#here-data-dates-and-time-periods)
- [HERE data at a glance](#here-data-at-a-glance)
- [Detailed dataset overview](#detailed-dataset-overview)
    - [Traffic data](#traffic-data)
        - [Data schema](#data-schema)
            - [Getting link attributes](#getting-link-attributes)
            - [Matching maps and data](#matching-maps-and-data)
            - [Functional classes](#functional-classes)
    - [GIS data](#gis-data)
    - [Traffic patterns: traffic models](#traffic-patterns-traffic-models)
    - [Routing with traffic data](#routing-with-traffic-data)
    - [Aggregating traffic data](#aggregating-traffic-data)
- [All the links to HERE data documentation (in one place)](#all-the-links-to-here-data-documentation-in-one-place)
- [But I still have questions!!!](#but-i-still-have-questions)

## What is HERE data?

HERE data is travel time data provided by HERE Technologies from a mix of vehicle probes. We have a daily [automated airflow pipeline](traffic/README.md) that pulls 5-min aggregated speed data for each link in the city from the here API. For streets classified collectors and above, we aggregate up to segments using the [congestion network](https://github.com/CityofToronto/bdit_congestion/tree/grid/congestion_grid) and produce [summary tables](https://github.com/CityofToronto/bdit_congestion/blob/data_aggregation/congestion_data_aggregation/sql/generate_segments_tti_weekly.sql) with indices such as Travel Time Index and Buffer Index.

<details>
    <summary>Travel Time Index</summary>
    TTI is the ratio of the average travel time and free-flow speeds. For example, a TTI of 1.3 indicates a 20-minute free-flow trip requires 26 minutes.
</details>

<details>
    <summary>Buffer Index</summary>
    Indicates the additional time for unexpected delays that commuters should consider along with average travel time. For example, if BI and average travel time are 20% and 10 minutes, then the buffer time would be 2 minutes. Since it is calculated by 95th percentile travel time, it represents almost all worst-case delay scenarios and assures travelers to be on-time 95 percent of all trips.
</details>

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
- [traffic analytics](#traffic-data) tables containing probe data for all Toronto streets
- [traffic patterns](#traffic-patterns-traffic-models) tables containing aggregated speed information for all Toronto streets
- [street attribute tables](#getting-link-attributes) showing speed limits, street names one way info (and more!) for Toronto's streets
- [many GIS layers](#gis-data) of streets, intersections, points of interest, trails, rail corridors and water features.

## HERE Data: dates and time periods
As stated above, the traffic analytics table (here.ta) is updated daily. 

Other products are generally updated quarterly, though this schedule has been somewhat disrupted due to the pandemic.

There are also aggregated tables or views specific to certain modes (like trucks or cars) or time periods (like weekends or night-time).

We have HERE data going back to 2012, but there sure weren't as many people driving around with "probes" in their pockets back then! Therefore, 2014 is generally seen as the first year for which travel times can be reasonably relied upon. 

Though data are reported at 5-minute intervals, on any given link (aka road segment) there may not be a lot of observations. Longer time periods and roads with higher traffic volumes have more probe data, and therefore more accurate results, than short time periods on roads with light traffic. More probe data increases accuracy. There are some road segments in the City (cul-de-sacs and other lightly travelled roads) that have very few observations. Because of this, we calculate travel times for time periods that are at least three weeks or longer so that data from multiple days can be aggregated into a more accurate result.

# HERE data at a glance

The Question       | The Answer     |
:----------------- | :---------------
What is the HERE dataset used for? | To calculate travel times and monitor congestion (mostly)
Where is the HERE dataset from? | HERE Technologies, via an agreement with Transport Canada
Is it available on Open Data? | No
What area does the HERE dataset represent? | All of Toronto; the co-ordinate system is EPSG: 4326
Where is it stored? | On an internal postgres database called bigdata; in several schema (here, here_analysis, here_eval and here_gis)
Are there any naming conventions? | If you see `_##_#` at the end of a table name (like `streets_21_1`) the first number is the year, and the second number is the revision (which usually corresponds to the quarter).
How often is it updated? | Probe data are updated every day; reference files are usually updated quarterly
How long are the time bins? | 5 minutes
How far back does it go? | To 2012, but 2014 data are much more accurate
What are the limitations? | Travel times are generally calculated for time periods lasting three or more weeks; use data from 2014 onward
I work for the City - what can I get? | Raw data - observations for all links in 5-minute bins. We can also put together custom aggregations.
I don't work for the City - what can I get? | Aggregated data (custom aggregations may be possible depending on the intended use of the data).
Are raw data available? | Yes (if you work for the City)
Who can I contact about HERE data? | Email us at transportationdata@toronto.ca

# Detailed dataset overview

## Traffic data

Historical data are acquired through the Traffic Analytics download portal. Data goes back to `2012-01-01` and are aggregated in 5-minute bins.In our database the data points are stored in partitioned tables under `here.ta` (fun fact: the "ta" stands for traffic analytics)! Data are loaded on a daily basis using the python command line application described [here](traffic/README.md).

### Data Schema for `here.ta`

| column | type | indexed | description |
| ------ | ---- | ------- | ----------- |
| link_dir | text | ✓ | Unique link id, per direction |
| tx | timestamp | | Timestamp of _start_ of 5-minute observation bin |
| dt | date | ✓ | Date of 5-minute observation bin; matches `tx` |
| tod | time | ✓ | Time of 5-minute observation bin; matches `tx` |
| length | integer | | Link length in meters, rounded to integer |
| mean | numeric(4,1) | | Arithmetic mean of observed speed(s) in the 5-minute bin weighted by the amount of data coming from the probe |
| stddev | numeric(4,1) | | standard deviation of the observed speed(s) |
| min_spd | integer | | Observed minimum speed |
| max_spd | integer | | Observed maximum speed |
| pct_50 | integer | | Observed median speed |
| pct_85 | integer | | Observed 85th percentile speed - use with caution as sample sizes (of vehicles) are very small within 5-minute bins |
| confidence | integer | | proprietary measure derived from `stddev` and `sample_size`; higher values mean greater 'confidence' in reliability of `mean` |
| sample_size | integer | | the number of probe vehicles traversing a segment within a 5-minute bin **plus** the number of 'probe samples' |

For an exploratory description of coverage (or how much probe data there is) for our roads, check out [this notebook](https://github.com/CityofToronto/bdit_team_wiki/blob/here_evaluation/here_evaluation/Descriptive_eval.ipynb).

#### Getting link attributes

The Traffic Analytics `link_dir` is a concatenation of the `streets` layer `link_id` and a `travel direction` character (F,T). `F` and `T` represent "From" and "To" relative to each link's reference node, which is *always* the node with the lowest latitude. In the case of two nodes with equal latitude, the node with the lowest longitude is the reference node. To join `ta` data to the `here_gis.streets_att` table use the following:

```sql
JOIN here_gis.streets_att_16_1 gis ON gis.link_id = LEFT(ta.link_dir, -1)::numeric
```

#### Matching maps and data

HERE updates their maps on an annual basis (usually) to keep up with Toronto's evolving street network. It's important to make sure that the traffic analytics data you're using matches the street network. For 2019 to August 2022, the `here.ta` table corresponds to the `here.routing_streets_21_1` layer. You can check to see if the `here.ta` layer corresponds to any given map layer by joining the attributes using the `link_dir` field and examining the results.

#### Functional classes

HERE groups roads into five functional classes, labelled 1 to 5. Lower numbers are used to represent roads with high volumes of traffic (so highways would fall under functional class 1 while local roads would have a functional class of 4).

Functional class 5 contains a little bit of everything that doesn't fall into the other classes. Currently exclude

`"paved"  = 'Y' AND "poiaccess" =  'N' AND "ar_auto" = 'Y' AND "ar_traff" = 'Y'`

## GIS data

A lot of map layers provided by HERE, see the [README](gis/README.md) in the [gis](gis/) folder for more info.

## Traffic patterns: traffic models

Just like the sun doesn't always shine, the streets of Toronto don't always produce vehicle probe speeds. In those cases, HERE provides us with traffic patterns, a model for each street link by time of week. Check [this README](traffic_patterns/README.md) for more info.

## Routing with traffic data

One use of historical traffic data is the ability to route a vehicle from an
arbitrary point to another arbitrary point using traffic data **at that point
in time**. Since our data are already in a database, this can be accomplished
using the [`pgRouting`](http://pgrouting.org/) PostgreSQL extension. It is
necessary to have [traffic patterns](#traffic-patterns-traffic-models) loaded
to fill in gaps in traffic data in time.

The following views prepare the HERE data for routing (code found
[here](traffic/sql/create_here_routing.sql)):

- `here.routing_nodes_YY_R`: a view of all intersections derived from the `z_levels_YY_R`
  gis layer.
- `here.routing_streets_YY_R`: The geography of streets is provided as
  centerlines, but traffic is provided directionally. This view creates
  directional links for each permitted travel direction on a navigable street
  with a `geom` drawn in the direction of travel.

Its a good idea to make sure that your tables or views are from the same time period, or as close to the same time period, as possible. Due to some inconsistencies in what we receive from HERE, perfect time period matches are not always possible. For example, as of July 2022:
- the latest traffic pattern dataset that we have is for 2019 (the 15-min table is called `here.traffic_pattern_19_spd_15`);
- our latest street + intersection networks are for Q1 of 2021 (`here_gis.streets_21_1` and `here_gis.z_levels_21_1`, respectively); and,
- we have probe data from two days ago (in `here.ta`, via the partitioned table `here.ta_202207`).

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

## Aggregating traffic data

HERE Traffic time data is at a link and 5-min resolution but, for data requests and projects we typically aggregate them up to a segment or over a certain time period. Check out this [documentation](https://github.com/CityofToronto/bdit_data-sources/blob/master/here/here_aggregation.md) to learn more about aggregating here data.

Custom aggregations can take hours to generate. Using aggregate tables can really help speed up the process!

# All the links to HERE data documentation (in one place)
...by order of appearance in this readme...
* [Procedure for loading new data](traffic/README.md) 
* [Traffic patterns: traffic models](traffic_patterns/README.md)
* [HERE GIS datasets](gis/README.md)
* [Aggregating HERE data](here_aggregation.md)

# But I still have questions!!!
Awesome! We love talking about data. Further inquiries about HERE data should be sent to transportationdata@toronto.ca.  