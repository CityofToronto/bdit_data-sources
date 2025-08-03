DROP SCHEMA gtfs CASCADE;
CREATE SCHEMA gtfs;

CREATE TABLE gtfs.feed_info (
feed_id serial,
insert_date timestamptz
);


CREATE TABLE gtfs.calendar_dates_imp (
service_id smallint not null,
date_ DATE not null ,
exception_type smallint not null,
feed_id int
);

CREATE TABLE gtfs.calendar_imp(
service_id smallint not null,
 monday boolean not null,
 tuesday boolean not null,
 wednesday boolean not null,
 thursday boolean not null,
 friday boolean not null,
 saturday boolean not null,
 sunday boolean not null,
 start_date DATE not null,
 end_date dATE not null,
feed_id int
);

CREATE TABLE gtfs.calendar_dates  (
service_id int,
date_ BIGINT ,
exception_type  int,
feed_id int
);


CREATE TABLE gtfs.calendar (
service_id INT ,
monday INT,
tuesday INT,
wednesday INT,
thursday INT,
friday INT,
saturday INT,
sunday INT,
start_date bigint ,
end_date bigint ,
feed_id INT 
);

CREATE TABLE gtfs.routes(
route_id int PRIMARY KEY,
agency_id smallint NOT NULL,
route_short_name TEXT NOT NULL,
route_long_name TEXT NOT NULL,
route_desc TEXT,
route_type smallint NOT NULL,
route_url TEXT,
route_color  CHAR(6) NOT NULL,
route_text_color CHAR(6),
feed_id int
);

CREATE TABLE gtfs.stop_times(
trip_id bigint NOT NULL,
arrival_time interval NOT NULL,
departure_time interval NOT NULL,
stop_id int NOT NULL,
stop_sequence smallint NOT NULL,
stop_headsign TEXT ,
pickup_type smallint NOT NULL,
drop_off_type smallint NOT NULL,
shape_dist_traveled numeric(7,4) DEFAULT 0 ,
feed_id int
);

CREATE TABLE gtfs.stops(
stop_id INT PRIMARY KEY,
stop_code TEXT NOT NULL,
stop_name TEXT NOT NULL,
stop_desc TEXT ,
stop_lat TEXT NOT NULL,
stop_lon TEXT NOT NULL,
geom GEOMETRY(Point, 4326), 
zone_id SMALLINT,
stop_url TEXT,
location_type TEXT ,
parent_station INT ,
stop_timezone TEXT, --This is null but it's in the feed
wheelchair_boarding SMALLINT ,
feed_id int
);

CREATE TABLE gtfs.shapes (
shape_id BIGINT NOT NULL,
shape_pt_lat TEXT NOT NULL,
shape_pt_lon TEXT NOT NULL,
shape_pt_sequence INT NOT NULL,
shape_dist_traveled numeric(7,4) DEFAULT 0 ,
feed_id int
);

CREATE TABLE gtfs.shapes_geom(
shape_id BIGINT NOT NULL,
feed_id int,
geom GEOMETRY(LineString, 4326)

);

CREATE TABLE gtfs.trips(
route_id INT NOT NULL,
service_id SMALLINT NOT NULL,
trip_id BIGINT NOT NULL,
trip_headsign TEXT ,
trip_short_name TEXT,
direction_id SMALLINT NOT NULL,
block_id BIGINT,
shape_id INT NOT NULL,
wheelchair_accessible SMALLINT,
feed_id int
);

ALTER TABLE gtfs.trips ADD COLUMN bikes_allowed INTEGER;
