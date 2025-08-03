/*Creates views to display only the most recent GTFS schedule for each table type
*/

SET search_path = gtfs, pg_catalog;


CREATE VIEW calendar_latest AS (

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
ORDER BY feed_id, end_date DESC
);



CREATE VIEW calendar_dates_latest AS (
SELECT 
    service_id,
    date_ ,
    exception_type
FROM calendar_dates_imp
INNER JOIN calendar_latest USING (feed_id, service_id)
);


GRANT SELECT ON  calendar_dates_imp TO bdit_humans;

CREATE VIEW routes_latest AS  (
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
INNER JOIN calendar_latest USING (feed_id)
);

GRANT SELECT ON  routes_latest TO bdit_humans;

CREATE VIEW shapes_geom_latest AS  (
SELECT 
    shape_id ,
    feed_id ,
    geom 
FROM shapes_geom
INNER JOIN calendar_latest USING (feed_id)
);

GRANT SELECT ON  shapes_geom_latest TO bdit_humans;

CREATE VIEW stop_times_latest AS  (
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
INNER JOIN calendar_latest USING (feed_id)
);

GRANT SELECT ON  stop_times_latest TO bdit_humans;

CREATE VIEW stops_latest AS  (
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
INNER JOIN calendar_latest USING (feed_id)
);

GRANT SELECT ON  stops_latest TO bdit_humans;

CREATE VIEW  trips_latest AS (
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
INNER JOIN calendar_latest USING (feed_id, service_id)
);

GRANT SELECT ON  trips_latest TO bdit_humans;