CREATE SCHEMA IF NOT EXISTS gtfs;

DROP TABLE IF EXISTS gtfs.agency;
CREATE TABLE gtfs.agency (
	agency_uid integer,
	agency_id character varying,
	agency_name character varying,
	agency_url character varying,
	agency_timezone character varying,
	agency_lang character varying,
	agency_phone character varying,
	agency_fare_url character varying,
	agency_email character varying
	);

 DROP TABLE IF EXISTS gtfs.stops;
 CREATE TABLE gtfs.stops (
	stop_uid integer,
	stop_id character varying,
	agency_uid integer,
	agency_id character varying,
	stop_code character varying,
	stop_name character varying,
	stop_desc character varying,
	stop_lat double precision,
	stop_lon double precision,
	zone_id character varying,
	stop_url character varying,
	location_type integer,
	parent_station character varying,
	stop_timezone character varying,
	wheelchair_boarding varchar,
	geom geometry
	);

DROP TABLE IF EXISTS gtfs.routes;
CREATE TABLE gtfs.routes (
	route_uid integer,
	route_id character varying,
	agency_uid integer,
	agency_id character varying,
	route_short_name character varying,
	route_long_name character varying,
	route_desc character varying,
	route_type integer,
	route_url character varying,
	route_color character varying,
	route_text_color character varying
	);

DROP TABLE IF EXISTS gtfs.route_types;
CREATE TABLE gtfs.route_types (
	route_type integer,
	description character varying
);
INSERT INTO gtfs.route_types VALUES (0, 'Tram, Streetcar, Light rail');
INSERT INTO gtfs.route_types VALUES (1, 'Subway, Metro');
INSERT INTO gtfs.route_types VALUES (2, 'Rail');
INSERT INTO gtfs.route_types VALUES (3, 'Bus');
INSERT INTO gtfs.route_types VALUES (4, 'Ferry');
INSERT INTO gtfs.route_types VALUES (5, 'Cable car');
INSERT INTO gtfs.route_types VALUES (6, 'Gondola, Suspended cable car');
INSERT INTO gtfs.route_types VALUES (7, 'Funicular');

DROP TABLE IF EXISTS gtfs.trips;
CREATE TABLE gtfs.trips (
	trip_uid integer,
	trip_id character varying,
	agency_id character varying,
	route_uid integer,
	route_id character varying,
	service_uid integer,
	service_id character varying,
	trip_headsign character varying,
	trip_short_name character varying,
	direction_id integer,
	block_id character varying,
	shape_id character varying,
	wheelchair_accessible integer,
	bikes_allowed integer
	);

DROP TABLE IF EXISTS gtfs.stop_times;
CREATE TABLE gtfs.stop_times (
	times_uid integer,
	agency_id character varying,
	trip_uid integer,
	trip_id character varying,
	stop_uid integer,
	stop_id character varying,
	arrival_time time without time zone,
	departure_time time without time zone,
	stop_sequence integer,
	stop_headsign character varying,
	pickup_type integer,
	drop_off_type integer,
	shape_dist_traveled double precision,
	timepoint integer
	);

DROP TABLE IF EXISTS gtfs.calendar;
CREATE TABLE gtfs.calendar (
	service_uid integer,
	service_id character varying,
	agency_id character varying,
	monday boolean,
	tuesday boolean,
	wednesday boolean,
	thursday boolean,
	friday boolean,
	saturday boolean,
	sunday boolean
	);

DROP TABLE IF EXISTS gtfs.calendar_dates;
CREATE TABLE gtfs.calendar_dates (
	service_uid integer,
	service_id character varying,
	agency_id character varying,
	dt date,
	exception_type integer
	);
	
DROP TABLE IF EXISTS gtfs.fare_attributes;
CREATE TABLE gtfs.fare_attributes (
	fare_uid integer,
	fare_id character varying,
	agency_id character varying,
	price double precision,
	currency_type character varying,
	payment_method integer,
	transfers integer,
	transfer_duration integer);

DROP TABLE IF EXISTS gtfs.fare_rules;
CREATE TABLE gtfs.fare_rules (
	fare_uid integer,
	fare_id character varying,
	agency_id character varying,
	route_uid integer,
	route_id character varying,
	origin_id character varying,
	destination_id character varying,
	contains_id character varying
	);

DROP TABLE IF EXISTS gtfs.shapes;
CREATE TABLE gtfs.shapes (
	shape_uid integer,
	shape_id character varying,
	agency_id character varying,
	shape_pt_lat double precision,
	shape_pt_lon double precision,
	shape_pt_sequence integer,
	shape_dist_traveled double precision,
	geom geometry
	);

-- gtfs.frequencies code here

DROP TABLE IF EXISTS gtfs.transfers;
CREATE TABLE gtfs.transfers (
	transfer_uid integer,
	agency_id character varying,
	from_stop_uid integer,
	from_stop_id character varying,
	to_stop_uid integer,
	to_stop_id character varying,
	transfer_type integer,
	min_transfer_time integer
	);

-- gtfs.feed_info code here