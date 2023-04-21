# Getting Started with Collision Data

Welcome to the tutorial on querying collision data! This tutorial assumes you are familiar with Postgres and PostGIS. If you'd like resources on those, please see the Data & Analytics Postgres onboarding documentation [here](https://www.notion.so/bditto/PostgreSQL-Exercises-322493ab085b442f96bfdb77b039cfca).

## Layout

`collisions_replicator.events` is a table of all data related to the collision event, such as its location and time of day, as opposed to data related to involved individuals, such as injury or manoeuver. 

`collisions_replicator.involved` includes data for all individuals involved in collisions - "involved data" for short.

`collisions_replicator.acc_safe_copy` is a direct mirror of the table on the MOVE server, and has not been processed - the category codes are all numbers - not text. It should not be used for analysis - use the `events` and `involved` tables instead. `acc_safe_copy` is typically only examined for diagnostic purposes.

## Joining Events and Involved

THe `events` and `involved` tables are often joined to get a single table, where each row represents an individual involved in a crash, but will contain data at both event and involved levels.

To join the two tables together, we use:

```sql
SELECT *
FROM collisions_replicator.events
LEFT JOIN collisions_replicator.involved USING (collision_no);
```

This will return *every* record available, so typically we also append a `WHERE` clause to restrict the query by time and place. 

## Counting Events and Involved

One common query is for the total number of collision events, or the total number of individuals involved in collisions, aggregated to the month, or year. If we, for example, wanted the number of collision events and involved from 2015-2019 inclusive, we'd do:

```sql
SELECT 
    ev.accyear,
    COUNT(DISTINCT ev.collision_no) AS n_collisions, -- Number of collision **events**
    COUNT(inv.*) AS n_involved -- Number of people **involved**
FROM collisions_replicator.events AS ev
LEFT JOIN collisions_replicator.involved AS inv USING (collision_no)
WHERE 
    ev.accyear >= 2015 
    AND ev.accyear < 2020
GROUP BY ev.accyear
ORDER BY 1;
```

As of 2023-04-11, the output looks like:

accyear | n_collisions | n_involved
-- | -- | --
2015 | 50863 | 124230
2016 | 55632 | 108298
2017 | 58906 | 105704
2018 | 62354 | 108209
2019 | 64343 | 108340

but be aware that due to the ever-refreshing nature of collisions mentioned in the [Readme.md](Readme.md), these numbers will surely change by small amounts with time.

The `events.data_source` column lists whether the collision event comes from TPS or CRC (or cannot be deduced from the `ACCNB`). `involved.validation_userid` gives the name of Data & Analytics staff member who most recently validated the involved (sometimes only some individals from a collision event are validated). If we want the number of events, involved and validated involved from the data subdivided by year and data source, we'd do:

```sql
SELECT 
    ev.accyear,
    ev.data_source,
    COUNT(DISTINCT ev.collision_no) AS n_collisions, -- Number of collision **events**
    COUNT(inv.*) AS n_involved, -- Number of people **involved**
    COUNT(*) FILTER (WHERE inv.validation_userid IS NOT NULL) AS n_valid_involved -- Number of people **involved** whose data has been validated
FROM collisions_replicator.events AS ev
LEFT JOIN collisions_replicator.involved AS inv USING (collision_no)
WHERE 
    ev.accyear >= 2015 
    AND ev.accyear < 2020
GROUP BY ev.accyear, ev.data_source
ORDER BY 1, 2;
```

As of 2023-04-11, the output looks like:

accyear | data_source | n_collisions | n_involved | n_valid_involved
-- | -- | -- | -- | --
2015 | CRC | 36057 | 89333 | 38933
2015 | TPS | 14806 | 34897 | 29082
2016 | CRC | 44102 | 80667 | 34872
2016 | TPS | 11506 | 27600 | 22618
2016 | NULL | 24 | 31 | 0
2017 | CRC | 48968 | 81550 | 28562
2017 | TPS | 9922 | 24126 | 20630
2017 | NULL | 16 | 28 | 2
2018 | CRC | 52651 | 84579 | 57001
2018 | TPS | 9691 | 23609 | 18084
2018 | NULL | 12 | 21 | 5
2019 | CRC | 55107 | 85991 | 17836
2019 | TPS | 9204 | 22299 | 12043
2019 | NULL | 32 | 50 | 7

We see that the majority of collisions come from the CRC (since they handle minor collisions). Collisions from TPS are much more likely to be validated
(because they're much more likely to involved killed or seriously injured individuals, which are prioritized for validation). The number of validated
collisions goes down with year (since it takes time to validate).

## Summing Involved

Data request clients will often ask for data aggregated up to the collision event level, but also ask for involved-level data. One way to satisfy these
requests (though be sure to confirm with the client that this is acceptable to them!) is to sum up the number of involved of a particular category (see
[this data request](https://github.com/Toronto-Big-Data-Innovation-Team/bdit_data_requests/pull/140/)).

If we, for example, wanted the number of killed or seriously injured (KSI) per collisions on 2015 involving at least one KSI, and at least one person under the age of 18 (who may or may not be the KSI):

```sql
SELECT 
    ev.collision_no,
    ev.accdate,
    COUNT(inv.*) AS n_involved,
    COUNT(inv.*) FILTER (WHERE involved_injury_class IN ('MAJOR', 'FATAL')) AS n_involved_ksi
FROM collisions_replicator.events AS ev
LEFT JOIN collisions_replicator.involved AS inv USING (collision_no)
WHERE ev.accyear = 2015
GROUP BY ev.collision_no, ev.accdate
HAVING COUNT(inv.*) FILTER (WHERE inv.involved_age < 18) > 0 AND COUNT(inv.*) FILTER (WHERE inv.involved_injury_class IN ('MAJOR', 'FATAL')) > 0
ORDER BY 1, 2;
```

The first five rows are:

collision_no | accdate | n_involved | n_involved_ksi
-- | -- | -- | --
1585594 | 2015-01-02 | 6 | 1
1586728 | 2015-01-30 | 7 | 1
1587840 | 2015-02-24 | 4 | 1
1588650 | 2015-03-17 | 4 | 1
1589108 | 2015-04-01 | 3 | 1

## Geospatial Transformations of Collisions

`collisions_replicator.events` includes a `geom` column (SRID 4326) based off of the `latitude` and `longitude` columns. No attempt has been made to clean bad lon-lats (in particular, those where `latitude` or `longitude` are close to zero, rather than 43 and -79, respectively), so they'll need to be cleaned either by removing latitudes and longitudes nowhere near Toronto, or by joining with another geometry. The latter is often done for data requests.

Let's say we wanted to associate all 2015-2019 inclusive collisions that occurred along Yonge St. between Bloor and Dundas. We can generate a street geometry using the `gis.text_to_centreline_geom` function (documented [here](https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/text_to_centreline)),
and then buffer the geometry to spatially join with the collisions.

```sql
WITH raw_geom AS (
	-- Create a geometry by joining Yonge centreline segments from Dundas to Bloor.
	SELECT 
        gis.text_to_centreline_geom('Yonge Street', 'Dundas Street West', 'Bloor Street West') AS street_geom
    ), 

-- Buffer the geometry out 20 m. Note that we had to transform to SRID 2952 to do this, since the units of SRID 4326 are degrees.
buffered_geom AS (
	SELECT 
        ST_TRANSFORM(ST_BUFFER(ST_TRANSFORM(rg.street_geom, 2952), 20), 4326) AS street_geom
	FROM raw_geom AS rg
)

SELECT ev.collision_no,
       ev.geom
FROM buffered_geom AS bg
LEFT JOIN collisions_replicator.events AS ev ON ST_CONTAINS(bg.street_geom, ev.geom)
-- Use ST_CONTAINS (https://postgis.net/docs/ST_Contains.html) for spatial association.
WHERE ev.accyear = 2015;
```

The first five lines returned are:

collision_no | geom
-- | --
1549570 | 0101000020E6100000AAD903ADC0D853C0E6762FF7C9D54540
1549852 | 0101000020E6100000BF66B96C74D853C0001C7BF65CD44540
1549872 | 0101000020E6100000B5368DEDB5D853C0CBB9145795D54540
1551137 | 0101000020E610000044FAEDEBC0D853C022A98592C9D54540
1551443 | 0101000020E6100000F71F990E9DD853C01B9DF3531CD54540

Notice that we transformed the street geometry back to SRID 4326 prior to spatially joining with the collisions. If our street geometry were in another spatial referencing system, we would have to transform the `geom` column of `collisions_replicator.events` to join with it. [CTEs and subqueries](https://gis.stackexchange.com/a/194036), once generated, do not use the indexes of their parent tables, so spatially joining two CTEs together will require a sequential rather than an index scan, which could slow the join down by orders of magnitude. To avoid this, either:
- Do not spatially join the outputs large CTEs/subqueries.
- Separate large CTEs/subqueries out as temporary tables, then create spatial
  indexes for these tables before spatially joining with them.

## Nuances of Collision Locations

### Spatial Joining

You may have noticed that we used a 20 meter buffer in the previous example, and may be wondering if this is a standard definition used when joining street and collision geometries together; it is not. Indeed, *there is currently no standard practice for spatially associating collisions with other geometries*. Instead, you are expected to either define a buffer - with the help of clients for data requests - or perform sensitivity testing to ensure that an acceptable minority of collisions are being left out of the join. 20 meters is typical of arterial streets, but larger numbers should be used for exceptionally wide streets like St. Clair West, or for highways, while smaller values may be used for local roads.

When joining collisions with a network of buffered streets, we typically use a single buffer width for all streets, and performs sensitivity testing to make sure that the buffer width is wide enough that an acceptable minority of collisions are left out, but narrow enough that collisions not on the street
network are being spuriously associated.

You may be required to produce a one-to-one association between collision and road network (i.e. a collision can only be assigned to one road segment in the
network). In that case, consider a multi-step process:
- First, associate collisions with buffered street segments.
- Then, calculate the orthogonal distance between the collision and *unbuffered*
  street segments.
- Finally, associate the collision with the closest-distance street segment.

For an example of this process in action, see the
[pattern](https://github.com/Toronto-Big-Data-Innovation-Team/bdit_vz_analysis/blob/d60503c00ca821558532a1a52cfbdb6f8e8ff0f8/network_screening/roadscreen/roadscreen/ingest.py#L802) for producing collision/midblock associations for the Vision Zero pedestrian midblock crossing network screening. In particular, `multi_conflation_1` and `multi_conflation_2` use the process above to produce a one-to-one association.

### Using Geolocation Versus `stname`, `location_type`, `location_class`, `traffic_control`, or `px`

There are a large number of columns that encode collision location in some way (the list in the title is incomplete). For all sorts of reasons they are not all consistent with one-another. Please see the collision coding manual in the [Manuals page in Notion](https://www.notion.so/bditto/ca4e026b4f20474cbb32ccfeecf9dd76?v=a9428dc0fb3447e5b9c1427f8868e7c8) for details.

Here are a few factors to consider:
- Geolocation (lon/lat) is usually assumed to take precedence over other quantities, though it is possible for a collision to be mis-geocoded either in the original TPS/CRC report, or during validation.
- `stname1` is the dominant road the collision, and `stname2` the cross-road. For midblocks `stname2` is `NULL`. For intersections, `stname1` is the name of the road with the higher functional class (eg. for a collision at the intersection of arterial road Eglinton Ave W. and local road Maxwell Ave, `stname1` will be `EGLINTON` and `stname2` will be `MAXWELL`).
- `location_type` (named `ACCLOC` in `collisions_replicator.acc_safe_copy`) comes from the original TPS and CRC reports. `location_class` (`LOCOORD` in `collisions_replicator.acc`) is a simplified version that conforms to Transportation Services standards. Notably `LOCOORD` is not defined in the collision coding manual. You should be careful when using either to select for collisions at intersections or along midblocks. For example, `location_class` may be labeled `MID-BLOCK` if a crash occurs just after a vehicle clears the intersection but is still only a few metres away. These ambiguities need to be better documented than they are now (since some of the details appear to be passed down orally from staff member to staff member).
- `traffic_control` lists the traffic control system most relevant to the collision (though does not need to be a factor of the collision itself). For example, if at a signalized intersection, a vehicle is exiting a driveway (30 metres within) and strikes a cyclist on the sidewalk, the control is `NO CONTROL`.
- `px` indicates the geographic association between a collision and a signalized intersection (so collisions with a non-null `px` may have `traffic_control = NO CONTROL`). If a client is interested in all collisions geographically close to signalized intersections (regardless if they were being controlled by the signal), it is better to query using `px` than `traffic_control`.

For an example of querying using a combination of these variables, see the query to generate the `signalized_twodriver_collisions` temporary table [here](https://github.com/Toronto-Big-Data-Innovation-Team/bdit_data_requests/issues/144#issuecomment-829498207).

Due to these complexities, it is imperative that any analyst *perform thorough quality control checks* when selecting by location. It may even be prudent to
select using two separate combinations of variables to see which produces a better dataset. This is especially true when reusing queries that filter using one or more location columns - please be *very* careful when doing that!
