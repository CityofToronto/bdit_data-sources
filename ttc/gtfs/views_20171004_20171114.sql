SET search_path = gtfs_raph, pg_catalog;

CREATE VIEW calendar_20171004 AS (

SELECT DISTINCT ON (feed_id) service_id, monday,
    tuesday ,
    wednesday ,
    thursday ,
    friday ,
    saturday ,
    sunday ,
    start_date ,
    end_date,
    feed_id
FROM calendar_imp
WHERE '2017-10-04' BETWEEN start_date AND end_date AND wednesday
ORDER BY feed_id, end_date DESC
);



CREATE VIEW calendar_dates_20171004 AS (
SELECT 
    service_id,
    date_ ,
    exception_type
FROM calendar_dates_imp
INNER JOIN calendar_20171004 USING (feed_id, service_id)
);


GRANT SELECT ON  calendar_dates_imp TO bdit_humans;

CREATE VIEW routes_20171004 AS  (
SELECT 
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    route_type,
    route_url,
    route_color,
    route_text_color
FROM routes
INNER JOIN calendar_20171004 USING (feed_id)
);

GRANT SELECT ON  routes_20171004 TO bdit_humans;

CREATE VIEW shapes_geom_20171004 AS  (
SELECT 
    shape_id ,
    feed_id ,
    geom 
FROM shapes_geom
INNER JOIN calendar_20171004 USING (feed_id)
);

GRANT SELECT ON  shapes_geom_20171004 TO bdit_humans;

CREATE VIEW stop_times_20171004 AS  (
SELECT
    trip_id,
    arrival_time,
    departure_time,
    stop_id,
    stop_sequence,
    stop_headsign,
    pickup_type,
    drop_off_type,
    shape_dist_traveled
FROM stop_times
INNER JOIN calendar_20171004 USING (feed_id)
);

GRANT SELECT ON  stop_times_20171004 TO bdit_humans;

CREATE VIEW stops_20171004 AS  (
SELECT 
    stop_id,
    stop_code,
    stop_name,
    stop_desc,
    stop_lat,
    stop_lon,
    geom,
    zone_id,
    stop_url,
    location_type,
    parent_station,
    wheelchair_boarding
FROM stops
INNER JOIN calendar_20171004 USING (feed_id)
);

GRANT SELECT ON  stops_20171004 TO bdit_humans;

CREATE VIEW  trips_20171004 AS (
SELECT 
    route_id,
    service_id,
    trip_id,
    trip_headsign,
    trip_short_name,
    direction_id,
    block_id,
    shape_id,
    wheelchair_accessible
FROM trips
INNER JOIN calendar_20171004 USING (feed_id, service_id)
);

GRANT SELECT ON  trips_20171004 TO bdit_humans;

CREATE VIEW calendar_20171114 AS (

SELECT DISTINCT ON (feed_id) service_id, monday,
    tuesday ,
    wednesday ,
    thursday ,
    friday ,
    saturday ,
    sunday ,
    start_date ,
    end_date,
    feed_id
FROM calendar_imp
WHERE '2017-11-14' BETWEEN start_date AND end_date AND wednesday
ORDER BY feed_id, end_date DESC
);



CREATE VIEW calendar_dates_20171114 AS (
SELECT 
    service_id,
    date_ ,
    exception_type
FROM calendar_dates_imp
INNER JOIN calendar_20171114 USING (feed_id, service_id)
);


GRANT SELECT ON  calendar_dates_imp TO bdit_humans;

CREATE VIEW routes_20171114 AS  (
SELECT 
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    route_type,
    route_url,
    route_color,
    route_text_color
FROM routes
INNER JOIN calendar_20171114 USING (feed_id)
);

GRANT SELECT ON  routes_20171114 TO bdit_humans;

CREATE VIEW shapes_geom_20171114 AS  (
SELECT 
    shape_id ,
    feed_id ,
    geom 
FROM shapes_geom
INNER JOIN calendar_20171114 USING (feed_id)
);

GRANT SELECT ON  shapes_geom_20171114 TO bdit_humans;

CREATE VIEW stop_times_20171114 AS  (
SELECT
    trip_id,
    arrival_time,
    departure_time,
    stop_id,
    stop_sequence,
    stop_headsign,
    pickup_type,
    drop_off_type,
    shape_dist_traveled
FROM stop_times
INNER JOIN calendar_20171114 USING (feed_id)
);

GRANT SELECT ON  stop_times_20171114 TO bdit_humans;

CREATE VIEW stops_20171114 AS  (
SELECT 
    stop_id,
    stop_code,
    stop_name,
    stop_desc,
    stop_lat,
    stop_lon,
    geom,
    zone_id,
    stop_url,
    location_type,
    parent_station,
    wheelchair_boarding
FROM stops
INNER JOIN calendar_20171114 USING (feed_id)
);

GRANT SELECT ON  stops_20171114 TO bdit_humans;

CREATE VIEW  trips_20171114 AS (
SELECT 
    route_id,
    service_id,
    trip_id,
    trip_headsign,
    trip_short_name,
    direction_id,
    block_id,
    shape_id,
    wheelchair_accessible
FROM trips
INNER JOIN calendar_20171114 USING (feed_id, service_id)
);

GRANT SELECT ON  trips_20171114 TO bdit_humans;