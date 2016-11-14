CREATE SCHEMA IF NOT EXISTS parking;

DROP TABLE IF EXISTS parking.lots_main;
CREATE TABLE parking.lots_main (
	id integer,
	address character varying,
	lat double precision,
	lng double precision,
	rate character varying,
	carpark_type character varying,
	carpark_type_str character varying,
	is_ttc boolean,
	rate_half_hour double precision,
	capacity integer,
	max_height double precision
	);

 DROP TABLE IF EXISTS parking.lots_payment_methods;
 CREATE TABLE parking.lots_payment_methods (
	id integer,
	method character varying
	);

 DROP TABLE IF EXISTS parking.lots_payment_options;
 CREATE TABLE parking.lots_payment_options (
	id integer,
	opt character varying
	);

 DROP TABLE IF EXISTS parking.lots_periods;
 CREATE TABLE parking.lots_periods (
	id integer,
	period_id integer,
	period character varying
	);

DROP TABLE IF EXISTS parking.lots_rates;
CREATE TABLE parking.lots_rates (
	period_id integer,
	time_period character varying,
	rate character varying
	);
	