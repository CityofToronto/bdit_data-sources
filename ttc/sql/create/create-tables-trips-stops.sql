DROP TABLE IF EXISTS ttc.trips;
DROP TABLE IF EXISTS ttc.stops;
DROP TABLE IF EXISTS ttc.trip_stops;

CREATE TABLE ttc.trips (
	trip_uid serial,
	date_key integer,
	trip_id integer,
	line_number smallint,
	line_name text,
	pattern_name text,
	direction text,
	vehicle_id smallint,
	dt_start timestamp without time zone,
	dt_end timestamp without time zone);

CREATE TABLE ttc.stops (
	stop_uid serial,
	stop_name text,
	direction text,
	stop_latitude numeric,
	stop_longitude numeric);

CREATE TABLE ttc.trip_stops (
	trip_stop_uid serial,
	trip_uid bigint,
	stop_uid int,
	seq int,
	dep_time timestamp without time zone);

INSERT INTO ttc.trips(date_key, trip_id, line_number, line_name, pattern_name, direction, vehicle_id, dt_start, dt_end)
SELECT DISTINCT date_key, trip_id, line_number, line_name, pattern_name, TRIM(direction_name) AS direction, vehicle_id, MIN(departure_datetime) AS dt_start, MAX(departure_datetime) AS dt_end
FROM ttc.pointstop
GROUP BY date_key, trip_id, line_number, line_name, pattern_name, direction_name, vehicle_id
ORDER BY MIN(departure_datetime);

INSERT INTO ttc.stops(stop_name, direction, stop_latitude, stop_longitude)
SELECT DISTINCT stop_name, TRIM(direction_name) as direction, stop_latitude, stop_longitude
FROM ttc.pointstop;

INSERT INTO ttc.trip_stops(trip_uid, stop_uid, seq, dep_time)
SELECT B.trip_uid, C.stop_uid, ROW_NUMBER() OVER (PARTITION BY trip_uid ORDER BY A.departure_datetime) AS seq, A.departure_datetime AS dep_time
FROM ttc.pointstop A
INNER JOIN ttc.trips B USING (date_key, trip_id)
INNER JOIN ttc.stops C USING (stop_name, direction, stop_latitude, stop_longitude);
