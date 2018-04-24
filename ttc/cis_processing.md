# The CIS processing procedure

## Determine Direction issue #89

## Route 514

### Step 1: Created the table that having the CIS data of route 514 on Oct.04, 2017
Also, since the name of the time column in `ttc.cis_2017` is long, I shorten it to `date_time`.

```sql
SELECT message_datetime AS date_time, route, run, vehicle, latitude, longitude, position
INTO dzou2.dd_cis_514_1004
FROM ttc.cis_2017
WHERE route = 514 AND date(message_datetime) = '2017-10-04'
```

### Step 2: Filtered the CIS GPS points into Toronto areas by using a spatial frame.

The frame covers the areas like this:

!['toronto'](gtfs/img/toronto_frame.png)

```sql
WITH geo_frame AS (
SELECT ST_GeomFromText (
'POLYGON((-79.53854563237667 43.58741045774194,-79.50009348393917 43.59586487156128,-79.49674608708858 43.60065103517571,-79.49271204473018 43.60220490253136,
-79.48601725102901 43.60829567555957,-79.48464396001339 43.61767923091661,-79.47365763188839 43.629422147616,-79.46661951543331 43.634516244547484,
-79.45752146245479 43.63737371962997,-79.45074083806514 43.63675254095344,-79.44499018193721 43.6353859252612,-79.43314554692745 43.63091314750915,
-79.41460611821651 43.62917364403781,-79.40464975835323 43.63414352038869,-79.37941553594112 43.638616057718274,-79.3589878320837 43.64433048208388,
-79.34851648808956 43.63277684536143,-79.31435587407589 43.658489973755195,-79.27744867803096 43.671652821766116,-79.25727846623897 43.695115266992175,
-79.23627140523433 43.704578338242776,-79.1932916879797 43.74232267388661,-79.23591735364437 43.84004639555846,-79.61431267262935 43.75463018484762,
-79.53854563237667 43.58741045774194))', 4326) AS frame
),

io AS (
SELECT *, ST_Within (position, frame) AS inside
FROM geo_frame,dzou2.dd_cis_514_1004
)

SELECT * INTO dzou2.dd_cis_514_1004_f
FROM io WHERE inside = TRUE
```

The process filtered out 514 GPS points.

### Step 3: Created the table that having the CIS data and its angles with previous and next points.

```sql
SELECT rank() OVER (order by date_time) AS rank_time,
        date_time, run, vehicle, latitude, longitude, position,
        degrees(ST_Azimuth(position, lag(position,1) OVER (partition by run order by date_time))) AS angle_previous,
        degrees(ST_Azimuth(position, lag(position,-1) OVER (partition by run order by date_time))) AS angle_next
    INTO dzou2.dd_cis_514_angle
    FROM dzou2.dd_cis_514_1004_f
```

The first `angle_previous` and the last `angle_next` will be NULL, as well as the angles between two points which did not move.



### Step 4: Obtained the stop patterns and directions of route 514 on 10/04/2017 by Raph's query.

```sql
WITH distinct_stop_patterns AS (SELECT DISTINCT ON (shape_id, stop_sequence) shape_id, direction_id, stop_sequence, stop_id
                                FROM gtfs_raph.trips_20171004
                                NATURAL JOIN gtfs_raph.stop_times_20171004
                                NATURAL JOIN gtfs_raph.routes_20171004
                                WHERE route_short_name = '514'
                                ORDER BY shape_id, stop_sequence)

SELECT shape_id, direction_id, ARRAY_AGG(stop_name ORDER BY stop_sequence)
FROM distinct_stop_patterns
NATURAL JOIN gtfs_raph.stops_20171004
GROUP BY shape_id, direction_id
ORDER BY direction_id
```

Result:

shape_id | direction_id | stop_order
--- | --- | ---
691040 | 0 |DUFFERIN GATE LOOP""	DUFFERIN ST AT LIBERTY ST""	DUFFERIN ST AT KING ST WEST""	DUFFERIN AT KING""	KING ST WEST AT FRASER AVE""	KING ST WEST AT ATLANTIC AVE""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT SHAW ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT SPADINA AVE""	KING ST WEST AT BLUE JAYS WAY""	KING ST WEST AT JOHN ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT BAY ST""	KING ST WEST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT JARVIS ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST EAST AT ONTARIO ST EAST SIDE""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT TRINITY ST""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT SUMACH ST""	KING & SUMACH""	CHERRY ST AT FRONT ST EAST""	DISTILLERY LOOP""	DISTILLERY LOOP""}"
691041 | 0 | DUFFERIN GATE LOOP""	DUFFERIN ST AT LIBERTY ST""	DUFFERIN ST AT KING ST WEST""	DUFFERIN AT KING""	KING ST WEST AT FRASER AVE""	KING ST WEST AT ATLANTIC AVE""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT SHAW ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT SPADINA AVE""	KING ST WEST AT BLUE JAYS WAY""	KING ST WEST AT JOHN ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT BAY ST""	KING ST WEST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT JARVIS ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST EAST AT ONTARIO ST EAST SIDE""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""}"
691042 | 1 | DISTILLERY LOOP""	CHERRY ST AT FRONT ST EAST""	KING & SUMACH""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT TRINITY ST""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT ONTARIO ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST EAST AT JARVIS ST""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT VICTORIA ST""	KING ST EAST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST WEST AT BAY ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT JOHN ST""	KING ST WEST AT PETER ST""	KING ST WEST AT SPADINA AVE""	SPADINA AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT SHAW ST""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT JEFFERSON AVE""	KING ST WEST AT JOE SHUSTER WAY""	KING ST WEST AT DUFFERIN ST""	DUFFERIN AT KING""	DUFFERIN ST AT LIBERTY ST""	DUFFERIN ST AT SPRINGHURST AVE""	DUFFERIN GATE LOOP""	EXHIBITION WEST LOOP""}"
691043 | 1 | PARLIAMENT AT KING""	KING ST EAST AT ONTARIO ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST EAST AT JARVIS ST""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT VICTORIA ST""	KING ST EAST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST WEST AT BAY ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT JOHN ST""	KING ST WEST AT PETER ST""	KING ST WEST AT SPADINA AVE""	SPADINA AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT SHAW ST""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT JEFFERSON AVE""	KING ST WEST AT JOE SHUSTER WAY""	KING ST WEST AT DUFFERIN ST""	DUFFERIN AT KING""	DUFFERIN ST AT LIBERTY ST""	DUFFERIN ST AT SPRINGHURST AVE""	DUFFERIN GATE LOOP""	EXHIBITION WEST LOOP""}"


Thus, there are 2 patterns of the stops, and each pattern has 2 directions. Total 4 patterns.


### Step 5: Obtained the order and position for each stop based on last step.

```sql
WITH distinct_stop_patterns AS (SELECT DISTINCT ON (shape_id, stop_sequence) shape_id, direction_id, stop_sequence, stop_id
                                FROM gtfs_raph.trips_20171004
                                NATURAL JOIN gtfs_raph.stop_times_20171004
                                NATURAL JOIN gtfs_raph.routes_20171004
                                WHERE route_short_name = '514'
                                ORDER BY shape_id, stop_sequence)

SELECT shape_id, direction_id, stop_id, geom, stop_sequence
INTO dzou2.dd_514_stop_pattern
FROM distinct_stop_patterns
NATURAL JOIN gtfs_raph.stops_20171004
ORDER BY shape_id, stop_sequence
```

After running the query, the table named `dzou2.dd_514_stop_pattern` has the data of shape_id, direction_id, stop_name, and stop_sequence is the order of stops for each pattern.

### Step 6:  Created the table that having the GTFS stop data of route 514 on 10/04/2017 and its angles with previous and next points.

```sql
SELECT shape_id, direction_id, stop_id, geom, stop_sequence,
        degrees(ST_Azimuth(geom, lag(geom,1) OVER (partition by shape_id, direction_id order by stop_sequence))) AS angle_previous,
        degrees(ST_Azimuth(geom, lag(geom,-1) OVER (partition by shape_id, direction_id order by stop_sequence))) AS angle_next
INTO dzou2.dd_514_stop_angle
FROM dzou2.dd_514_stop_pattern

```

The terminal stops, first `angle_previous` and the last `angle_next` of each pattern will be NULL.


### Step 7: Add some columns that are needed in the next step

```sql
ALTER TABLE dd_cis_514_angle
ADD COLUMN id SERIAL PRIMARY KEY,
ADD COLUMN stop_id integer, ADD COLUMN direction_id smallint
```

### Step 8: Find the nearest GTFS stop for each CIS data and its direction

```sql
UPDATE dzou2.dd_cis_514_angle cis
SET stop_id = nearest.stop_id, direction_id = nearest.direction_id
FROM (SELECT b.id, stop_data.stop_id, stop_data.direction_id
      FROM dzou2.dd_cis_514_angle b
      CROSS JOIN LATERAL
	(SELECT stop_id, direction_id
         FROM dzou2.dd_514_stop_angle stops
         WHERE
         b.angle_previous IS NOT NULL
         AND
         ((stops.angle_previous IS NULL OR
           (b.angle_previous BETWEEN stops.angle_previous - 45 AND stops.angle_previous + 45))
           AND
           (b.angle_next IS NULL OR stops.angle_next IS NULL OR
           (b.angle_next BETWEEN stops.angle_next - 45 AND stops.angle_next + 45)))
        ORDER BY stops.geom <-> b.position LIMIT 1
        ) stop_data) nearest
WHERE nearest.id = cis.id
```

The query will only match the CIS data and their `direction_id` whose `angle_previous` IS NOT NULL.

There are two conditions when CIS data's `angle_previous` is null:

1. The first point.
2. The points that stay at the same position.

When `angle_previous` is null, it is difficult to determine its direction and its closest stop. In most cases, it matches a stop which we do not expect, and the result is that the "wrong matches" will affect the future steps of matching stops. The result will look like the streetcar turns its direction suddenly, so this step will ignore the data records that having `angle_previous` is null.


### Step 9: Finds the non-matches

```sql
SELECT * FROM dzou2.dd_cis_514_angle
WHERE direction_id IS NULL
```
There are 5567 rows outputted.


## Route 504

### Step 1: Get the CIS data of route 504 on 10/04/2017
```sql
SELECT message_datetime AS date_time, route, run, vehicle, latitude, longitude, position
INTO dzou2.dd_cis_504_1004
FROM ttc.cis_2017
WHERE route = 504 AND date(message_datetime) = '2017-10-04'
```
### Step 2: Filtered the CIS data outside Toronto by using same frame of Toronto.

```sql
WITH geo_frame AS (
SELECT ST_GeomFromText (
'POLYGON((-79.53854563237667 43.58741045774194,-79.50009348393917 43.59586487156128,-79.49674608708858 43.60065103517571,-79.49271204473018 43.60220490253136,
-79.48601725102901 43.60829567555957,-79.48464396001339 43.61767923091661,-79.47365763188839 43.629422147616,-79.46661951543331 43.634516244547484,
-79.45752146245479 43.63737371962997,-79.45074083806514 43.63675254095344,-79.44499018193721 43.6353859252612,-79.43314554692745 43.63091314750915,
-79.41460611821651 43.62917364403781,-79.40464975835323 43.63414352038869,-79.37941553594112 43.638616057718274,-79.3589878320837 43.64433048208388,
-79.34851648808956 43.63277684536143,-79.31435587407589 43.658489973755195,-79.27744867803096 43.671652821766116,-79.25727846623897 43.695115266992175,
-79.23627140523433 43.704578338242776,-79.1932916879797 43.74232267388661,-79.23591735364437 43.84004639555846,-79.61431267262935 43.75463018484762,
-79.53854563237667 43.58741045774194))', 4326) AS frame
),

io AS (
SELECT *, ST_Within (position, frame) AS inside
FROM geo_frame,dzou2.dd_cis_504_1004
)

SELECT * INTO dzou2.dd_cis_504_1004_f
FROM io WHERE inside = TRUE
```

### Step 3: Created the table that having the CIS data and its angles with previous and next points.

```sql
SELECT rank() OVER (order by date_time) AS rank_time,
        date_time, run, vehicle, latitude, longitude, position,
        degrees(ST_Azimuth(position, lag(position,1) OVER (partition by run order by date_time))) AS angle_previous,
        degrees(ST_Azimuth(position, lag(position,-1) OVER (partition by run order by date_time))) AS angle_next
    INTO dzou2.dd_cis_504_angle
    FROM dzou2.dd_cis_504_1004_f
```

The first `angle_previous` and the last `angle_next` will be NULL, as well as the angles between two points which did not move.

### Step 4: Gets the stop patterns and directions of route 504 on 10/04/2017

```sql
WITH distinct_stop_patterns AS (SELECT DISTINCT ON (shape_id, stop_sequence) shape_id, direction_id, stop_sequence, stop_id
                                FROM gtfs_raph.trips_20171004
                                NATURAL JOIN gtfs_raph.stop_times_20171004
                                NATURAL JOIN gtfs_raph.routes_20171004
                                WHERE route_short_name = '504'
                                ORDER BY shape_id, stop_sequence)

SELECT shape_id, direction_id, ARRAY_AGG(stop_name ORDER BY stop_sequence)
FROM distinct_stop_patterns
NATURAL JOIN gtfs_raph.stops_20171004
GROUP BY shape_id, direction_id
ORDER BY direction_id
```

Result:

shape_id | direction_id | stop_order
--- | --- | ---
690859 | 0 |DUNDAS WEST STATION""	RONCESVALLES AVE AT BOUSTEAD AVE SOUTH SIDE""	HOWARD PARK AT RONCESVALLES""	RONCESVALLES AVE AT HOWARD PARK AVE SOUTH SIDE""	RONCESVALLES AVE AT GRENADIER RD""	RONCESVALLES AVE AT HIGH PARK BLVD SOUTH SIDE""	RONCESVALLES AVE AT GALLEY AVE""	RONCESVALLES AVE AT MARION ST SOUTH SIDE""	RONCESVALLES AVE AT QUEEN ST WEST""	RONC. AT QUEEN""	KING ST WEST AT WILSON PARK RD""	KING ST WEST AT DOWLING AVE""	KING ST WEST AT JAMESON AVE""	KING ST WEST AT DUNN AVE""	KING ST WEST AT SPENCER AVE""	KING ST WEST AT DUFFERIN ST""	DUFFERIN AT KING""	KING ST WEST AT FRASER AVE""	KING ST WEST AT ATLANTIC AVE""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT SHAW ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT SPADINA AVE""	KING ST WEST AT BLUE JAYS WAY""	KING ST WEST AT JOHN ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT BAY ST""	KING ST WEST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT JARVIS ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST E AT ONTARIO ST""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT TRINITY ST""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT SUMACH ST""	KING ST E AT RIVER ST""	QUEEN ST EAST AT CARROLL ST""	QUEEN ST EAST AT BROADVIEW AVE""	BROADVIEW AT QUEEN""	BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW STATION""	BROADVIEW STATION""}"
690860 | 0 |EDNA AVE AT DUNDAS ST WEST""	RONCESVALLES AVE AT BOUSTEAD AVE SOUTH SIDE""	HOWARD PARK AT RONCESVALLES""	RONCESVALLES AVE AT HOWARD PARK AVE SOUTH SIDE""	RONCESVALLES AVE AT GRENADIER RD""	RONCESVALLES AVE AT HIGH PARK BLVD SOUTH SIDE""	RONCESVALLES AVE AT GALLEY AVE""	RONCESVALLES AVE AT MARION ST SOUTH SIDE""	RONCESVALLES AVE AT QUEEN ST WEST""	RONC. AT QUEEN""	KING ST WEST AT WILSON PARK RD""	KING ST WEST AT DOWLING AVE""	KING ST WEST AT JAMESON AVE""	KING ST WEST AT DUNN AVE""	KING ST WEST AT SPENCER AVE""	KING ST WEST AT DUFFERIN ST""	DUFFERIN AT KING""	KING ST WEST AT FRASER AVE""	KING ST WEST AT ATLANTIC AVE""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT SHAW ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT SPADINA AVE""	KING ST WEST AT BLUE JAYS WAY""	KING ST WEST AT JOHN ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT BAY ST""	KING ST WEST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT JARVIS ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST E AT ONTARIO ST""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT TRINITY ST""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT SUMACH ST""	KING ST E AT RIVER ST""	QUEEN ST EAST AT BROADVIEW AVE""	BROADVIEW AT QUEEN""	BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW STATION""	BROADVIEW STATION""}"
690863 | 0 | DUNDAS WEST STATION""	RONCESVALLES AVE AT BOUSTEAD AVE SOUTH SIDE""	HOWARD PARK AT RONCESVALLES""	RONCESVALLES AVE AT HOWARD PARK AVE SOUTH SIDE""	RONCESVALLES AVE AT GRENADIER RD""	RONCESVALLES AVE AT HIGH PARK BLVD SOUTH SIDE""	RONCESVALLES AVE AT GALLEY AVE""	RONCESVALLES AVE AT MARION ST SOUTH SIDE""	RONCESVALLES AVE AT QUEEN ST WEST""	RONC. AT QUEEN""	KING ST WEST AT WILSON PARK RD""	KING ST WEST AT DOWLING AVE""	KING ST WEST AT JAMESON AVE""	KING ST WEST AT DUNN AVE""	KING ST WEST AT SPENCER AVE""	KING ST WEST AT DUFFERIN ST""	DUFFERIN AT KING""	KING ST WEST AT FRASER AVE""	KING ST WEST AT ATLANTIC AVE""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT SHAW ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT SPADINA AVE""	KING ST WEST AT BLUE JAYS WAY""	KING ST WEST AT JOHN ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT BAY ST""	KING ST WEST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT JARVIS ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST E AT ONTARIO ST""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT TRINITY ST""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT SUMACH ST""	KING ST E AT RIVER ST""	QUEEN ST EAST AT CARROLL ST""	QUEEN ST EAST AT BROADVIEW AVE""	BROADVIEW AT QUEEN""	BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW STATION""	BROADVIEW STATION""}"
690864 | 0 | BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW STATION""	BROADVIEW STATION""}"
690866 | 0 |DUNDAS WEST STATION""	RONCESVALLES AVE AT BOUSTEAD AVE SOUTH SIDE""	HOWARD PARK AT RONCESVALLES""	RONCESVALLES AVE AT HOWARD PARK AVE SOUTH SIDE""	RONCESVALLES AVE AT GRENADIER RD""	RONCESVALLES AVE AT HIGH PARK BLVD SOUTH SIDE""	RONCESVALLES AVE AT GALLEY AVE""	RONCESVALLES AVE AT MARION ST SOUTH SIDE""	RONCESVALLES AVE AT QUEEN ST WEST""	RONC. AT QUEEN""}"
690867 | 0 |RONC. AT QUEEN""	KING ST WEST AT WILSON PARK RD""	KING ST WEST AT DOWLING AVE""	KING ST WEST AT JAMESON AVE""	KING ST WEST AT DUNN AVE""	KING ST WEST AT SPENCER AVE""	KING ST WEST AT DUFFERIN ST""	DUFFERIN AT KING""	KING ST WEST AT FRASER AVE""	KING ST WEST AT ATLANTIC AVE""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT SHAW ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT SPADINA AVE""	KING ST WEST AT BLUE JAYS WAY""	KING ST WEST AT JOHN ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT BAY ST""	KING ST WEST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT JARVIS ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST E AT ONTARIO ST""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT TRINITY ST""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT SUMACH ST""	KING ST E AT RIVER ST""	QUEEN ST EAST AT CARROLL ST""	QUEEN ST EAST AT BROADVIEW AVE""	BROADVIEW AT QUEEN""	BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW STATION""	BROADVIEW STATION""}"
690870 | 1 |BROADVIEW STATION""	ERINDALE AVE AT BROADVIEW AVE""	BROADVIEW AVE AT DANFORTH AVE""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT JACK LAYTON WAY""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT QUEEN ST EAST""	BROADVIEW AT QUEEN""	QUEEN ST EAST AT CARROLL ST""	KING ST EAST AT RIVER ST""	KING ST EAST AT SUMACH ST""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT TRINITY ST""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT ONTARIO ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST EAST AT JARVIS ST""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST WEST AT BAY ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT JOHN ST""	KING ST WEST AT PETER ST""	KING ST WEST AT SPADINA AVE""	SPADINA AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT SHAW ST""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT JEFFERSON AVE""	KING ST WEST AT JOE SHUSTER WAY""	KING ST WEST AT DUFFERIN ST""	DUFFERIN AT KING""	KING ST WEST AT SPENCER AVE""	KING ST WEST AT DUNN AVE""	KING ST WEST AT JAMESON AVE""	KING ST WEST AT DOWLING AVE""	KING ST WEST AT WILSON PARK RD WEST SIDE""	RONCESVALLES AVE AT QUEEN ST WEST""	RONC. AT QUEEN""	RONCESVALLES AVE AT QUEEN ST WEST NORTH SIDE""	RONCESVALLES AVE AT MARION ST NORTH SIDE""	RONCESVALLES AVE AT GARDEN AVE""	RONCESVALLES AVE AT FERMANAGH AVE""	RONCESVALLES AVE AT GRENADIER RD""	RONCESVALLES AVE AT HOWARD PARK AVE""	HOWARD PARK AT RONCESVALLES""	RONCESVALLES AVE AT BOUSTEAD AVE NORTH SIDE""	DUNDAS ST WEST AT BLOOR ST WEST""	DUNDAS WEST STATION""	DUNDAS WEST STATION""}"
690873 | 1 | BROADVIEW STATION""	ERINDALE AVE AT BROADVIEW AVE""	BROADVIEW AVE AT DANFORTH AVE""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT JACK LAYTON WAY""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT QUEEN ST EAST""	BROADVIEW AT QUEEN""	QUEEN ST EAST AT CARROLL ST""	KING ST EAST AT RIVER ST""	KING ST EAST AT SUMACH ST""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT TRINITY ST""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT ONTARIO ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST EAST AT JARVIS ST""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST WEST AT BAY ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT JOHN ST""	KING ST WEST AT PETER ST""	KING ST WEST AT SPADINA AVE""	SPADINA AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT SHAW ST""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT JEFFERSON AVE""	KING ST WEST AT JOE SHUSTER WAY""	DUFFERIN AT KING""	KING ST WEST AT SPENCER AVE""	KING ST WEST AT DUNN AVE""	KING ST WEST AT JAMESON AVE""	KING ST WEST AT DOWLING AVE""	KING ST WEST AT WILSON PARK RD WEST SIDE""	RONCESVALLES AVE AT QUEEN ST WEST""	RONC. AT QUEEN""}"
690880 | 1 |BROADVIEW STATION""	ERINDALE AVE AT BROADVIEW AVE""	BROADVIEW AVE AT DANFORTH AVE""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT JACK LAYTON WAY""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT QUEEN ST EAST""	BROADVIEW AT QUEEN""	QUEEN ST EAST AT CARROLL ST""	KING ST EAST AT RIVER ST""	KING ST EAST AT SUMACH ST""	KING ST EAST AT SACKVILLE ST""	KING ST EAST AT TRINITY ST""	KING ST EAST AT PARLIAMENT ST""	PARLIAMENT AT KING""	KING ST EAST AT ONTARIO ST""	KING ST EAST AT SHERBOURNE ST""	KING ST. E AT SHERBOURNE ST.""	KING ST EAST AT JARVIS ST""	KING ST EAST AT CHURCH ST""	CHURCH AT KING""	KING ST EAST AT YONGE ST (KING STATION)""	YONGE AT KING""	KING ST WEST AT BAY ST""	KING ST WEST AT UNIVERSITY AVE (ST ANDREW STATION)""	KING ST WEST AT JOHN ST""	KING ST WEST AT PETER ST""	KING ST WEST AT SPADINA AVE""	SPADINA AT KING""	KING ST WEST AT PORTLAND ST""	KING ST WEST AT BATHURST ST""	BATHURST AT KING""	KING ST WEST AT TECUMSETH ST""	KING ST WEST AT NIAGARA ST""	KING ST WEST AT STRACHAN AVE""	KING ST WEST AT SHAW ST""	KING ST WEST AT SUDBURY ST""	KING ST WEST AT JEFFERSON AVE""	KING ST WEST AT JOE SHUSTER WAY""	KING ST WEST AT DUFFERIN ST""	DUFFERIN AT KING""	KING ST WEST AT SPENCER AVE""	KING ST WEST AT DUNN AVE""	KING ST WEST AT JAMESON AVE""	KING ST WEST AT DOWLING AVE""	KING ST WEST AT WILSON PARK RD WEST SIDE""	RONCESVALLES AVE AT QUEEN ST WEST""	RONC. AT QUEEN""	RONCESVALLES AVE AT QUEEN ST WEST NORTH SIDE""	RONCESVALLES AVE AT MARION ST NORTH SIDE""	RONCESVALLES AVE AT GARDEN AVE""	RONCESVALLES AVE AT FERMANAGH AVE""	RONCESVALLES AVE AT GRENADIER RD""	RONCESVALLES AVE AT HOWARD PARK AVE""	HOWARD PARK AT RONCESVALLES""	RONCESVALLES AVE AT BOUSTEAD AVE NORTH SIDE""	DUNDAS ST WEST AT BLOOR ST WEST""	DUNDAS WEST STATION""	DUNDAS WEST STATION""}"
690881 | 1 |BROADVIEW STATION""	ERINDALE AVE AT BROADVIEW AVE""	BROADVIEW AVE AT DANFORTH AVE""	BROADVIEW AVE AT WOLFREY AVE""	BROADVIEW AVE AT MILLBROOK CRES""	BROADVIEW AVE AT WITHROW AVE""	BROADVIEW AVE AT LANGLEY AVE-BRIDGEPOINT HEALTH CTR""	BROADVIEW AVE AT JACK LAYTON WAY""	BROADVIEW AVE AT GERRARD ST EAST""	BROADVIEW AVE AT MOUNT STEPHEN ST""	BROADVIEW AVE AT DUNDAS ST EAST""	BROADVIEW AVE AT QUEEN ST EAST""	BROADVIEW AT QUEEN""}"
690882 | 1 |RONCESVALLES AVE AT QUEEN ST WEST NORTH SIDE""	RONCESVALLES AVE AT MARION ST NORTH SIDE""	RONCESVALLES AVE AT GARDEN AVE""	RONCESVALLES AVE AT FERMANAGH AVE""	RONCESVALLES AVE AT GRENADIER RD""	RONCESVALLES AVE AT HOWARD PARK AVE""	HOWARD PARK AT RONCESVALLES""	RONCESVALLES AVE AT BOUSTEAD AVE NORTH SIDE""	DUNDAS ST WEST AT BLOOR ST WEST""	DUNDAS WEST STATION""	DUNDAS WEST STATION""}"

Thus, there are 11 patterns of stops of route 504, and 6 of them are in the direction 0, while 5 of them are in the direction of 1.

### Step 5: Obtained the order and position for each stop based on last step.

```sql
WITH distinct_stop_patterns AS (SELECT DISTINCT ON (shape_id, stop_sequence) shape_id, direction_id, stop_sequence, stop_id
                                FROM gtfs_raph.trips_20171004
                                NATURAL JOIN gtfs_raph.stop_times_20171004
                                NATURAL JOIN gtfs_raph.routes_20171004
                                WHERE route_short_name = '504'
                                ORDER BY shape_id, stop_sequence)

SELECT shape_id, direction_id, stop_id, geom, stop_sequence
INTO dzou2.dd_504_stop_pattern
FROM distinct_stop_patterns
NATURAL JOIN gtfs_raph.stops_20171004
ORDER BY shape_id, stop_sequence
```

### Step 6:  Created the table that having the GTFS stop data of route 514 on 10/04/2017 and its angles with previous and next points.

```sql
SELECT shape_id, direction_id, stop_id, geom, stop_sequence,
        degrees(ST_Azimuth(geom, lag(geom,1) OVER (partition by shape_id, direction_id order by stop_sequence))) AS angle_previous,
        degrees(ST_Azimuth(geom, lag(geom,-1) OVER (partition by shape_id, direction_id order by stop_sequence))) AS angle_next
INTO dzou2.dd_504_stop_angle
FROM dzou2.dd_504_stop_pattern
```

The terminal stops, first `angle_previous` and the last `angle_next` of each pattern will be NULL.


### Step 7: Add some columns that are needed in the next step

```sql
ALTER TABLE dd_cis_504_angle
ADD COLUMN id SERIAL PRIMARY KEY,
ADD COLUMN stop_id integer, ADD COLUMN direction_id smallint
```

### Step 8: Find the nearest GTFS stop for each CIS data and its direction

```sql
UPDATE dzou2.dd_cis_504_angle cis
SET stop_id = nearest.stop_id, direction_id = nearest.direction_id
FROM (SELECT b.id, stop_data.stop_id, stop_data.direction_id
      FROM dzou2.dd_cis_504_angle b
      CROSS JOIN LATERAL
	(SELECT stop_id, direction_id
         FROM dzou2.dd_504_stop_angle stops
         WHERE
         b.angle_previous IS NOT NULL
         AND
         ((stops.angle_previous IS NULL OR
           (b.angle_previous BETWEEN stops.angle_previous - 45 AND stops.angle_previous + 45))
           AND
           (b.angle_next IS NULL OR stops.angle_next IS NULL OR
           (b.angle_next BETWEEN stops.angle_next - 45 AND stops.angle_next + 45)))
        ORDER BY stops.geom <-> b.position LIMIT 1
        ) stop_data) nearest
WHERE nearest.id = cis.id
```

### Step 9: Finds the non-matches

```sql
SELECT * FROM dzou2.dd_cis_504_angle
WHERE direction_id IS NULL
```
There are 28679 rows outputted.



## Match Stops issue #88

## Route 514

### Step 1 & 2:
ST_LineLocatePoint to get the GPS point on the appropriate shapes_geom, then compared distance along line with matched stop to see if GPS record is before or after stop,

```sql
WITH
line_data AS(
SELECT geom AS line, direction_id FROM gtfs_raph.shapes_geom_20171004
    INNER JOIN gtfs_raph.trips_20171004 USING (shape_id)
    WHERE shape_id IN (691040 OR 691042)
    GROUP BY line, shape_id, direction_id
    ORDER BY shape_id
)

SELECT date_time, id AS cis_id, stop_id, a.direction_id,
ST_LineLocatePoint(line, position) AS cis_to_line, vehicle,
ST_LineLocatePoint(line, geom) AS stop_to_line,
(CASE WHEN ST_LineLocatePoint(line, position) > ST_LineLocatePoint(line, geom)
      THEN 'after'
      WHEN ST_LineLocatePoint(line, position) < ST_LineLocatePoint(line, geom)
      THEN 'before'
      WHEN ST_LineLocatePoint(line, position) = ST_LineLocatePoint(line, geom)
      THEN 'same'
      END) AS line_position,
ST_Distance(position::geography, geom::geography) AS distance
FROM line_data a, dzou2.dd_cis_514_angle b
INNER JOIN gtfs_raph.stops_20171004 USING (stop_id)
WHERE a.direction_id = b.direction_id
ORDER BY direction_id, date_time
```

### Step 3:
Get arrival time and departure time within 200m upstream, 10m downstream as per above data, and grouped them at each stop.

First of all, we need a sequence of counting.

```sql
CREATE SEQUENCE stops START 100
```

Then, validating the sequence.

```sql
SELECT nextval('stops')
```

At the same window:

```sql
WITH line_data AS(
SELECT geom AS line, direction_id FROM gtfs_raph.shapes_geom_20171004
    INNER JOIN gtfs_raph.trips_20171004 USING (shape_id)
    WHERE shape_id IN (691040, 691042)
    GROUP BY line, shape_id, direction_id
    ORDER BY shape_id
),

cis_gtfs AS(
SELECT date_time, id AS cis_id, stop_id, vehicle, a.direction_id,
ST_LineLocatePoint(line, position) AS cis_to_line,
ST_LineLocatePoint(line, geom) AS stop_to_line,
(CASE WHEN ST_LineLocatePoint(line, position) > ST_LineLocatePoint(line, geom)
      THEN 'after'
      WHEN ST_LineLocatePoint(line, position) < ST_LineLocatePoint(line, geom)
      THEN 'before'
      WHEN ST_LineLocatePoint(line, position) = ST_LineLocatePoint(line, geom)
      THEN 'same'
      END) AS line_position,
ST_Distance(position::geography, geom::geography) AS distance
FROM line_data a, dzou2.dd_cis_514_angle b
INNER JOIN gtfs_raph.stops_20171004 USING (stop_id)
WHERE a.direction_id = b.direction_id
ORDER BY vehicle, a.direction_id, date_time
),

stop_orders AS (
SELECT *,
(CASE WHEN lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time) IS NULL
      THEN nextval('stops')
      WHEN stop_id <> lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time)
      THEN nextval('stops')
      WHEN stop_id = lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time)
      THEN currval('stops')
END) AS stop_order
FROM cis_gtfs
WHERE (line_position = 'before' AND distance <= 200) OR (line_position = 'after' AND distance <= 10) OR (line_position = 'same' AND distance <= 100)
ORDER BY vehicle, direction_id, date_time
)

SELECT MIN(date_time) AS arrival_time, MAX(date_time) AS departure_time, vehicle, stop_id, direction_id, array_agg(DISTINCT cis_id) AS cis_group
INTO dzou2.match_stop_514
FROM stop_orders
GROUP BY stop_order, vehicle, stop_id, direction_id
```

It outputs the `arrival_time` and `departure_time` for each vehicle in each stop in directions, and representing the earliest time record the GPS records at 200m upstream of the stop and lastest time record at 10m downstream.

The heat maps of the result is on `bdit_data-sources/ttc/validating_cis_processing.ipynb`.


## Route 504

### Step 1 & 2:
ST_LineLocatePoint to get the GPS point on the appropriate shapes_geom, then compared distance along line with matched stop to see if GPS record is before or after stop,

```sql
WITH
line_data AS(
SELECT geom AS line, direction_id FROM gtfs_raph.shapes_geom_20171004
    INNER JOIN gtfs_raph.trips_20171004 USING (shape_id)
    WHERE shape_id IN (690863 OR 690880)
    GROUP BY line, shape_id, direction_id
    ORDER BY shape_id
)

SELECT date_time, id AS cis_id, stop_id, a.direction_id,
ST_LineLocatePoint(line, position) AS cis_to_line, vehicle,
ST_LineLocatePoint(line, geom) AS stop_to_line,
(CASE WHEN ST_LineLocatePoint(line, position) > ST_LineLocatePoint(line, geom)
      THEN 'after'
      WHEN ST_LineLocatePoint(line, position) < ST_LineLocatePoint(line, geom)
      THEN 'before'
      WHEN ST_LineLocatePoint(line, position) = ST_LineLocatePoint(line, geom)
      THEN 'same'
      END) AS line_position,
ST_Distance(position::geography, geom::geography) AS distance
FROM line_data a, dzou2.dd_cis_504_angle b
INNER JOIN gtfs_raph.stops_20171004 USING (stop_id)
WHERE a.direction_id = b.direction_id
ORDER BY direction_id, date_time
```

### Step 3:
Get arrival time and departure time within 200m upstream, 10m downstream as per above data, and grouped them at each stop.

The same as the steps for route 514:

```sql
CREATE SEQUENCE stops START 100
```

```sql
SELECT nextval('stops')
```

At the same window:

```sql
WITH line_data AS(
SELECT geom AS line, direction_id FROM gtfs_raph.shapes_geom_20171004
    INNER JOIN gtfs_raph.trips_20171004 USING (shape_id)
    WHERE shape_id IN (690863, 690880)
    GROUP BY line, shape_id, direction_id
    ORDER BY shape_id
),

cis_gtfs AS(
SELECT date_time, id AS cis_id, stop_id, vehicle, a.direction_id,
ST_LineLocatePoint(line, position) AS cis_to_line,
ST_LineLocatePoint(line, geom) AS stop_to_line,
(CASE WHEN ST_LineLocatePoint(line, position) > ST_LineLocatePoint(line, geom)
      THEN 'after'
      WHEN ST_LineLocatePoint(line, position) < ST_LineLocatePoint(line, geom)
      THEN 'before'
      WHEN ST_LineLocatePoint(line, position) = ST_LineLocatePoint(line, geom)
      THEN 'same'
      END) AS line_position,
ST_Distance(position::geography, geom::geography) AS distance
FROM line_data a, dzou2.dd_cis_504_angle b
INNER JOIN gtfs_raph.stops_20171004 USING (stop_id)
WHERE a.direction_id = b.direction_id
ORDER BY vehicle, a.direction_id, date_time
),

stop_orders AS (
SELECT *,
(CASE WHEN lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time) IS NULL
      THEN nextval('stops')
      WHEN stop_id <> lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time)
      THEN nextval('stops')
      WHEN stop_id = lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time)
      THEN currval('stops')
END) AS stop_order
FROM cis_gtfs
WHERE (line_position = 'before' AND distance <= 200) OR (line_position = 'after' AND distance <= 10) OR (line_position = 'same' AND distance <= 100)
ORDER BY vehicle, direction_id, date_time
)

SELECT MIN(date_time) AS arrival_time, MAX(date_time) AS departure_time, vehicle, stop_id, direction_id, array_agg(DISTINCT cis_id) AS cis_group
INTO dzou2.match_stop_504
FROM stop_orders
GROUP BY stop_order, vehicle, stop_id, direction_id
```

`bdit_data-sources/ttc/validating_cis_processing.ipynb` also shows the heat maps for route 504.

## Assign Trip IDs to CIS data #104

### Route 514 on 10/04/2017

First of all, we need a sequence of counting.

```sql
CREATE SEQUENCE cis_lst START 100
```

Then, validating the sequence.

```sql
SELECT nextval('cis_lst')
```

At the same window:

```sql
WITH
order_data AS (
SELECT arrival_time, departure_time, vehicle, stop_id, direction_id, cis_group,
rank() OVER (PARTITION BY vehicle ORDER BY arrival_time) AS order_id
FROM match_stop_514

ORDER BY vehicle, arrival_time
),

trips AS(
SELECT
(CASE WHEN lag(vehicle, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time) IS NULL
      THEN nextval('cis_lst')
      WHEN (direction_id <> lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN nextval('cis_lst')
      WHEN (direction_id = lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN currval('cis_lst')
END) AS trip_id,
arrival_time, departure_time, cis_group, direction_id, vehicle, stop_id
FROM order_data
ORDER BY vehicle, arrival_time
),

open_array AS (
SELECT trip_id, arrival_time, departure_time, unnest(cis_group) AS cis_id, direction_id, vehicle, stop_id
FROM trips
ORDER BY vehicle, arrival_time
),

good_trip_id AS(
SELECT trip_id, count(*), array_agg(cis_id) AS groups
FROM open_array
GROUP BY trip_id
HAVING count(*) > 10
ORDER BY count DESC)

SELECT a.trip_id, a.arrival_time, a.departure_time, a.cis_id, a.direction_id, a.vehicle, a.stop_id
INTO dzou2.trips_cis_514
FROM open_array a, good_trip_id b
WHERE a.trip_id = b.trip_id
```

The table `dzou2.trips_cis_514` stores the trip IDs assigned for each trip which has the more than 10 CIS data records, and `bdit_data-sources/ttc/validating_cis_processing.ipynb` has illustrated the reason of choosing 10 as the indicator.

### Route 504 on 10/04/2017

Using the same sequence,

```sql
SELECT nextval('cis_lst')
```

At the same window:

```sql
WITH
order_data AS (
SELECT arrival_time, departure_time, vehicle, stop_id, direction_id, cis_group,
rank() OVER (PARTITION BY vehicle ORDER BY arrival_time) AS order_id
FROM match_stop_504

ORDER BY vehicle, arrival_time
),

trips AS(
SELECT
(CASE WHEN lag(vehicle, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time) IS NULL
      THEN nextval('cis_lst')
      WHEN (direction_id <> lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN nextval('cis_lst')
      WHEN (direction_id = lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN currval('cis_lst')
END) AS trip_id,
arrival_time, departure_time, cis_group, direction_id, vehicle, stop_id
FROM order_data
ORDER BY vehicle, arrival_time
),

open_array AS (
SELECT trip_id, arrival_time, departure_time, unnest(cis_group) AS cis_id, direction_id, vehicle, stop_id
FROM trips
ORDER BY vehicle, arrival_time
),

good_trip_id AS(
SELECT trip_id, count(*), array_agg(cis_id) AS groups
FROM open_array
GROUP BY trip_id
HAVING count(*) >= 10
ORDER BY count DESC)

SELECT a.trip_id, a.arrival_time, a.departure_time, a.cis_id, a.direction_id, a.vehicle, a.stop_id
INTO dzou2.trips_cis_504
FROM open_array a, good_trip_id b
WHERE a.trip_id = b.trip_id
```

`bdit_data-sources/ttc/validating_cis_processing.ipynb` has illustrated the reason of filtering out the trips have less than 10 CIS data records.


## Trip filtering issue #87

#### Route 514 on 10/04/2017

### Step 1: Select the points that are in the pilot area (Bathurst to Jarvis).

Step 1.1: create a geoframe that represents the pilot area

!['Bathurst_Jarvis'](gtfs/img/Bathurst_Jarvis.png)

Step 1.2: Use the geoframe to filter the CIS trips

```sql
WITH geo_frame AS (
SELECT ST_GeomFromText (
'POLYGON((-79.40035615335398 43.63780813971652,-79.40593514810496 43.65184496379264,-79.38997064004832 43.655074211367626,
-79.37452111612254 43.658241189360304,-79.36928544412547 43.64513751014725,-79.40035615335398 43.63780813971652))', 4326) AS frame
),

trip_location AS (
SELECT a.*, b.position FROM dzou2.trips_cis_514 a
INNER JOIN dzou2.dd_cis_514_angle b ON a.cis_id = b.id
)

SELECT trip_id, arrival_time, departure_time, cis_id, direction_id, vehicle, stop_id, ST_Within(position, frame) AS pilot_area
INTO tf_cis_514
FROM geo_frame, trip_location
```

The table named `tf_cis_514` includes all the data from `trips_cis_514` and a column named `pilot_area` which indicates if the CIS data point is in the pilot area or not.

There are 9373 rows of data in `tf_cis_514` at this point.

### Step 2: Filtered out the trips linked to vehicles that spend less than 1 km or more than 4 km in the pilot area (Bathurst to Jarvis).

```sql
WITH trips AS(
SELECT trip_id, arrival_time, departure_time, a.direction_id, a.vehicle, a.stop_id, pilot_area,
ST_DistanceSphere(position,
lag(position,1) OVER (partition by trip_id order by arrival_time)) AS distance
FROM tf_cis_514 a
INNER JOIN dzou2.dd_cis_514_angle b ON (a.cis_id = b.id)
WHERE pilot_area = TRUE
),

total_d AS (
SELECT trip_id, MIN(arrival_time) AS begin_time, MAX(departure_time) AS end_time, direction_id,
vehicle, SUM(distance)/1000 AS total_distance_km
FROM trips
GROUP BY trip_id, direction_id, vehicle
ORDER BY trip_id, begin_time
),

fail_trip_id AS (
SELECT trip_id
FROM total_d
WHERE total_distance_km < 1 OR total_distance_km > 4
)

DELETE FROM tf_cis_514 a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id
```

The temporary table `fail_trip_id` stores the trip IDs which linked to vehicles that spend less than 1 km or more than 4 km in the pilot area (Bathurst to Jarvis). There are 6 trips fulfill the requirements, and 109 CIS data are affected after running the query.

There are 9264 rows of data in `tf_cis_514` at this point.

### Step 3: Filtered out the trips linked to vehicles that spend more than 100 minutes in the pilot area.

```sql
WITH trips AS(
SELECT trip_id, arrival_time, departure_time, a.direction_id, a.vehicle, a.stop_id, pilot_area
FROM tf_cis_514 a
INNER JOIN dzou2.dd_cis_514_angle b ON (a.cis_id = b.id)
WHERE pilot_area = TRUE
),

total_time AS (
SELECT trip_id, MIN(arrival_time) AS begin_time, MAX(departure_time) AS end_time,
EXTRACT(EPOCH FROM (MAX(departure_time) - MIN(arrival_time)))/60 AS time_diff,
direction_id, vehicle
FROM trips
GROUP BY trip_id, direction_id, vehicle
ORDER BY trip_id, begin_time
),

fail_trip_id AS (
SELECT trip_id
FROM total_time
WHERE time_diff > 100
)

DELETE FROM tf_cis_514 a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id
```

The column `time_diff` in the temporary table `total_time` is the time (in minutes) of a vehicle spent in the pilot area; however, there is not any trip spent more than 100 minutes in the pilot area, so 0 rows are affected.

There are 9264 rows of data in `tf_cis_514` at this point.

To know the longest time period for a vehicle to spend in the pilot area, using the query

```sql
WITH trips AS(
SELECT trip_id, arrival_time, departure_time, a.direction_id, a.vehicle, a.stop_id, pilot_area
FROM tf_cis_514 a
INNER JOIN dzou2.dd_cis_514_angle b ON (a.cis_id = b.id)
WHERE pilot_area = TRUE
),

total_time AS (
SELECT trip_id, MIN(arrival_time) AS begin_time, MAX(departure_time) AS end_time,
EXTRACT(EPOCH FROM (MAX(departure_time) - MIN(arrival_time)))/60 AS time_diff,
direction_id, vehicle
FROM trips
GROUP BY trip_id, direction_id, vehicle
ORDER BY trip_id, begin_time
)

SELECT MAX(time_diff) FROM total_time
```

The result output:

|max|
|---|
|31|

Thus, the maximum time for a vehicle to spend in the pilot area is 31 minutes (route 514 on 10/04/2017).

### Step 4: Filtered out the trips with run numbers between 60 and 89.

```sql
WITH fail_trip_id AS(
SELECT trip_id
FROM tf_cis_514 a
INNER JOIN dzou2.dd_cis_514_angle b ON (a.cis_id = b.id)
WHERE run BETWEEN 60 AND 89
)

DELETE FROM tf_cis_514 a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id
```

There is not a trip has the run number between 60 and 89, so 0 rows are affected after running the query.

To know the run  number for route 514 on 10/04/2017,

```sql
SELECT DISTINCT run FROM ttc.cis_20171004
WHERE route = 514
ORDER BY run
```

|run|
|---|
|50|
|51|
|52|
|53|
|54|
|55|
|56|
|57|
|58|
|59|

Thus, the run numbers of route 514 on 10/04/2017 are from 50 to 59.
