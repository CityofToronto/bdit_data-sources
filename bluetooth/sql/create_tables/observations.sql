CREATE TABLE IF NOT EXISTS bluetooth.observations (
	id			bigserial not null primary key,
	user_id			bigint,
	analysis_id 		integer,
	measured_time 		integer,
	measured_time_no_filter	integer,
	startpoint_number 	smallint,
	startpoint_name 	character varying(8),
	endpoint_number 	smallint,
	endpoint_name 		character varying(8),
	measured_timestamp 	timestamp without time zone,
	outlier_level 		smallint,
	cod 			bigint,
	device_class 		smallint
);


/*Loops through the months for which we currently have data in order to create a partitioned table for each month*/

DO $do$
DECLARE
	startdate DATE;
	yyyy TEXT := '2020';
	yyyymm TEXT;
	basetablename TEXT := 'observations_';
	tablename TEXT;
BEGIN

	FOR mm IN 01..12 LOOP
		startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
		IF mm < 10 THEN
			yyyymm:= yyyy||'0'||mm;
		ELSE
			yyyymm:= yyyy||''||mm;
		END IF;
		tablename:= basetablename||yyyymm;
		EXECUTE format($$CREATE TABLE bluetooth.%I 
			(PRIMARY KEY (id),
			CHECK (measured_timestamp > DATE '$$||startdate ||$$'AND measured_timestamp <= DATE '$$||startdate ||$$'+ INTERVAL '1 month'),
			UNIQUE(user_id, analysis_id, measured_time, measured_timestamp)
			) INHERITS (bluetooth.observations);
			ALTER TABLE bluetooth.%I
  				OWNER TO bt_admins;
			CREATE INDEX ON bluetooth.%I (analysis_id);
			CREATE INDEX ON bluetooth.%I (cod);
			CREATE INDEX ON bluetooth.%I USING brin(measured_timestamp);
			$$
			, tablename);
		
	END LOOP;
END;
$do$ LANGUAGE plpgsql
	

for 

CREATE TABLE IF NOT EXISTS bluetooth.observations_201401 (
	CONSTRAINT	pk_201401	primary key (id),
	CONSTRAINT	ck_201401	CHECK		(	measured_timestamp > TIMESTAMP '2014-01-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-02-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201401 ON bluetooth.observations_201401 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201402 (
	CONSTRAINT	pk_201402	primary key (id),
	CONSTRAINT	ck_201402	CHECK		(	measured_timestamp > TIMESTAMP '2014-02-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-03-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201402 ON bluetooth.observations_201402 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201403 (
	CONSTRAINT	pk_201403	primary key (id),
	CONSTRAINT	ck_201403	CHECK		(	measured_timestamp > TIMESTAMP '2014-03-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-04-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201403 ON bluetooth.observations_201403 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201404 (
	CONSTRAINT	pk_201404	primary key (id),
	CONSTRAINT	ck_201404	CHECK		(	measured_timestamp > TIMESTAMP '2014-04-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-05-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201404 ON bluetooth.observations_201404 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201405 (
	CONSTRAINT	pk_201405	primary key (id),
	CONSTRAINT	ck_201405	CHECK		(	measured_timestamp > TIMESTAMP '2014-05-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-06-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201405 ON bluetooth.observations_201405 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201406 (
	CONSTRAINT	pk_201406	primary key (id),
	CONSTRAINT	ck_201406	CHECK		(	measured_timestamp > TIMESTAMP '2014-06-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-07-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201406 ON bluetooth.observations_201406 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201407 (
	CONSTRAINT	pk_201407	primary key (id),
	CONSTRAINT	ck_201407	CHECK		(	measured_timestamp > TIMESTAMP '2014-07-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-08-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201407 ON bluetooth.observations_201407 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201408 (
	CONSTRAINT	pk_201408	primary key (id),
	CONSTRAINT	ck_201408	CHECK		(	measured_timestamp > TIMESTAMP '2014-08-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-09-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201408 ON bluetooth.observations_201408 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201409 (
	CONSTRAINT	pk_201409	primary key (id),
	CONSTRAINT	ck_201409	CHECK		(	measured_timestamp > TIMESTAMP '2014-09-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-10-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201409 ON bluetooth.observations_201409 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201410 (
	CONSTRAINT	pk_201410	primary key (id),
	CONSTRAINT	ck_201410	CHECK		(	measured_timestamp > TIMESTAMP '2014-10-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-11-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201410 ON bluetooth.observations_201410 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201411 (
	CONSTRAINT	pk_201411	primary key (id),
	CONSTRAINT	ck_201411	CHECK		(	measured_timestamp > TIMESTAMP '2014-11-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-12-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201411 ON bluetooth.observations_201411 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201412 (
	CONSTRAINT	pk_201412	primary key (id),
	CONSTRAINT	ck_201412	CHECK		(	measured_timestamp > TIMESTAMP '2014-12-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-01-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201412 ON bluetooth.observations_201412 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201501 (
	CONSTRAINT	pk_201501	primary key (id),
	CONSTRAINT	ck_201501	CHECK		(	measured_timestamp > TIMESTAMP '2015-01-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-02-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201501 ON bluetooth.observations_201501 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201502 (
	CONSTRAINT	pk_201502	primary key (id),
	CONSTRAINT	ck_201502	CHECK		(	measured_timestamp > TIMESTAMP '2015-02-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-03-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201502 ON bluetooth.observations_201502 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201503 (
	CONSTRAINT	pk_201503	primary key (id),
	CONSTRAINT	ck_201503	CHECK		(	measured_timestamp > TIMESTAMP '2015-03-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-04-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201503 ON bluetooth.observations_201503 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201504 (
	CONSTRAINT	pk_201504	primary key (id),
	CONSTRAINT	ck_201504	CHECK		(	measured_timestamp > TIMESTAMP '2015-04-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-05-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201504 ON bluetooth.observations_201504 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201505 (
	CONSTRAINT	pk_201505	primary key (id),
	CONSTRAINT	ck_201505	CHECK		(	measured_timestamp > TIMESTAMP '2015-05-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-06-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201505 ON bluetooth.observations_201505 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201506 (
	CONSTRAINT	pk_201506	primary key (id),
	CONSTRAINT	ck_201506	CHECK		(	measured_timestamp > TIMESTAMP '2015-06-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-07-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201506 ON bluetooth.observations_201506 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201507 (
	CONSTRAINT	pk_201507	primary key (id),
	CONSTRAINT	ck_201507	CHECK		(	measured_timestamp > TIMESTAMP '2015-07-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-08-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201507 ON bluetooth.observations_201507 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201508 (
	CONSTRAINT	pk_201508	primary key (id),
	CONSTRAINT	ck_201508	CHECK		(	measured_timestamp > TIMESTAMP '2015-08-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-09-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201508 ON bluetooth.observations_201508 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201509 (
	CONSTRAINT	pk_201509	primary key (id),
	CONSTRAINT	ck_201509	CHECK		(	measured_timestamp > TIMESTAMP '2015-09-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-10-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201509 ON bluetooth.observations_201509 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201510 (
	CONSTRAINT	pk_201510	primary key (id),
	CONSTRAINT	ck_201510	CHECK		(	measured_timestamp > TIMESTAMP '2015-10-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-11-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201510 ON bluetooth.observations_201510 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201511 (
	CONSTRAINT	pk_201511	primary key (id),
	CONSTRAINT	ck_201511	CHECK		(	measured_timestamp > TIMESTAMP '2015-11-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-12-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201511 ON bluetooth.observations_201511 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201512 (
	CONSTRAINT	pk_201512	primary key (id),
	CONSTRAINT	ck_201512	CHECK		(	measured_timestamp > TIMESTAMP '2015-12-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-01-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201512 ON bluetooth.observations_201512 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201601 (
	CONSTRAINT	pk_201601	primary key (id),
	CONSTRAINT	ck_201601	CHECK		(	measured_timestamp > TIMESTAMP '2016-01-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-02-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201601 ON bluetooth.observations_201601 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201602 (
	CONSTRAINT	pk_201602	primary key (id),
	CONSTRAINT	ck_201602	CHECK		(	measured_timestamp > TIMESTAMP '2016-02-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-03-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201602 ON bluetooth.observations_201602 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201603 (
	CONSTRAINT	pk_201603	primary key (id),
	CONSTRAINT	ck_201603	CHECK		(	measured_timestamp > TIMESTAMP '2016-03-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-04-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201603 ON bluetooth.observations_201603 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201604 (
	CONSTRAINT	pk_201604	primary key (id),
	CONSTRAINT	ck_201604	CHECK		(	measured_timestamp > TIMESTAMP '2016-04-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-05-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201604 ON bluetooth.observations_201604 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201605 (
	CONSTRAINT	pk_201605	primary key (id),
	CONSTRAINT	ck_201605	CHECK		(	measured_timestamp > TIMESTAMP '2016-05-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-06-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201605 ON bluetooth.observations_201605 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201606 (
	CONSTRAINT	pk_201606	primary key (id),
	CONSTRAINT	ck_201606	CHECK		(	measured_timestamp > TIMESTAMP '2016-06-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-07-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201606 ON bluetooth.observations_201606 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201607 (
	CONSTRAINT	pk_201607	primary key (id),
	CONSTRAINT	ck_201607	CHECK		(	measured_timestamp > TIMESTAMP '2016-07-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-08-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201607 ON bluetooth.observations_201607 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201608 (
	CONSTRAINT	pk_201608	primary key (id),
	CONSTRAINT	ck_201608	CHECK		(	measured_timestamp > TIMESTAMP '2016-08-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-09-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201608 ON bluetooth.observations_201608 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201609 (
	CONSTRAINT	pk_201609	primary key (id),
	CONSTRAINT	ck_201609	CHECK		(	measured_timestamp > TIMESTAMP '2016-09-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-10-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201609 ON bluetooth.observations_201609 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201610 (
	CONSTRAINT	pk_201610	primary key (id),
	CONSTRAINT	ck_201610	CHECK		(	measured_timestamp > TIMESTAMP '2016-10-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-11-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201610 ON bluetooth.observations_201610 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201611 (
	CONSTRAINT	pk_201611	primary key (id),
	CONSTRAINT	ck_201611	CHECK		(	measured_timestamp > TIMESTAMP '2016-11-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-12-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201611 ON bluetooth.observations_201611 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201612 (
	CONSTRAINT	pk_201612	primary key (id),
	CONSTRAINT	ck_201612	CHECK		(	measured_timestamp > TIMESTAMP '2016-12-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-12-01' + INTERVAL '1 Month')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201612 ON bluetooth.observations_201612 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201701 (
	CONSTRAINT	pk_201701	primary key (id),
	CONSTRAINT	ck_201701	CHECK		(	measured_timestamp > TIMESTAMP '2017-01-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-02-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201701 ON bluetooth.observations_201701 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201702 (
	CONSTRAINT	pk_201702	primary key (id),
	CONSTRAINT	ck_201702	CHECK		(	measured_timestamp > TIMESTAMP '2017-02-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-03-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201702 ON bluetooth.observations_201702 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201703 (
	CONSTRAINT	pk_201703	primary key (id),
	CONSTRAINT	ck_201703	CHECK		(	measured_timestamp > TIMESTAMP '2017-03-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-04-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201703 ON bluetooth.observations_201703 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201704 (
	CONSTRAINT	pk_201704	primary key (id),
	CONSTRAINT	ck_201704	CHECK		(	measured_timestamp > TIMESTAMP '2017-04-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-05-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201704 ON bluetooth.observations_201704 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201705 (
	CONSTRAINT	pk_201705	primary key (id),
	CONSTRAINT	ck_201705	CHECK		(	measured_timestamp > TIMESTAMP '2017-05-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-06-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201705 ON bluetooth.observations_201705 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201706 (
	CONSTRAINT	pk_201706	primary key (id),
	CONSTRAINT	ck_201706	CHECK		(	measured_timestamp > TIMESTAMP '2017-06-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-07-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201706 ON bluetooth.observations_201706 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201707 (
	CONSTRAINT	pk_201707	primary key (id),
	CONSTRAINT	ck_201707	CHECK		(	measured_timestamp > TIMESTAMP '2017-07-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-08-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201707 ON bluetooth.observations_201707 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201708 (
	CONSTRAINT	pk_201708	primary key (id),
	CONSTRAINT	ck_201708	CHECK		(	measured_timestamp > TIMESTAMP '2017-08-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-09-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201708 ON bluetooth.observations_201708 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201709 (
	CONSTRAINT	pk_201709	primary key (id),
	CONSTRAINT	ck_201709	CHECK		(	measured_timestamp > TIMESTAMP '2017-09-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-10-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201709 ON bluetooth.observations_201709 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201710 (
	CONSTRAINT	pk_201710	primary key (id),
	CONSTRAINT	ck_201710	CHECK		(	measured_timestamp > TIMESTAMP '2017-10-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-11-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201710 ON bluetooth.observations_201710 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201711 (
	CONSTRAINT	pk_201711	primary key (id),
	CONSTRAINT	ck_201711	CHECK		(	measured_timestamp > TIMESTAMP '2017-11-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-12-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201711 ON bluetooth.observations_201711 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201712 (
	CONSTRAINT	pk_201712	primary key (id),
	CONSTRAINT	ck_201712	CHECK		(	measured_timestamp > TIMESTAMP '2017-12-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-12-01' + INTERVAL '1 Month')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201712 ON bluetooth.observations_201712 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201801 (
	CONSTRAINT	pk_201801	primary key (id),
	CONSTRAINT	ck_201801	CHECK		(	measured_timestamp > TIMESTAMP '2018-01-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-02-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201801 ON bluetooth.observations_201801 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201802 (
	CONSTRAINT	pk_201802	primary key (id),
	CONSTRAINT	ck_201802	CHECK		(	measured_timestamp > TIMESTAMP '2018-02-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-03-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201802 ON bluetooth.observations_201802 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201803 (
	CONSTRAINT	pk_201803	primary key (id),
	CONSTRAINT	ck_201803	CHECK		(	measured_timestamp > TIMESTAMP '2018-03-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-04-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201803 ON bluetooth.observations_201803 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201804 (
	CONSTRAINT	pk_201804	primary key (id),
	CONSTRAINT	ck_201804	CHECK		(	measured_timestamp > TIMESTAMP '2018-04-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-05-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201804 ON bluetooth.observations_201804 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201805 (
	CONSTRAINT	pk_201805	primary key (id),
	CONSTRAINT	ck_201805	CHECK		(	measured_timestamp > TIMESTAMP '2018-05-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-06-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201805 ON bluetooth.observations_201805 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201806 (
	CONSTRAINT	pk_201806	primary key (id),
	CONSTRAINT	ck_201806	CHECK		(	measured_timestamp > TIMESTAMP '2018-06-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-07-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201806 ON bluetooth.observations_201806 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201807 (
	CONSTRAINT	pk_201807	primary key (id),
	CONSTRAINT	ck_201807	CHECK		(	measured_timestamp > TIMESTAMP '2018-07-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-08-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201807 ON bluetooth.observations_201807 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201808 (
	CONSTRAINT	pk_201808	primary key (id),
	CONSTRAINT	ck_201808	CHECK		(	measured_timestamp > TIMESTAMP '2018-08-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-09-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201808 ON bluetooth.observations_201808 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201809 (
	CONSTRAINT	pk_201809	primary key (id),
	CONSTRAINT	ck_201809	CHECK		(	measured_timestamp > TIMESTAMP '2018-09-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-10-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201809 ON bluetooth.observations_201809 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201810 (
	CONSTRAINT	pk_201810	primary key (id),
	CONSTRAINT	ck_201810	CHECK		(	measured_timestamp > TIMESTAMP '2018-10-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-11-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201810 ON bluetooth.observations_201810 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201811 (
	CONSTRAINT	pk_201811	primary key (id),
	CONSTRAINT	ck_201811	CHECK		(	measured_timestamp > TIMESTAMP '2018-11-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-12-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201811 ON bluetooth.observations_201811 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201812 (
	CONSTRAINT	pk_201812	primary key (id),
	CONSTRAINT	ck_201812	CHECK		(	measured_timestamp > TIMESTAMP '2018-12-01' 
							AND 	measured_timestamp <= TIMESTAMP '2018-12-01' + INTERVAL '1 Month')
	) INHERITS (bluetooth.observations);
CREATE INDEX IF NOT EXISTS idx_ai_201812 ON bluetooth.observations_201812 (analysis_id);