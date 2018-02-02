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

CREATE TABLE IF NOT EXISTS bluetooth.observations_201401 (
	CONSTRAINT	pk_201401	primary key (id),
	CONSTRAINT	ck_201401	CHECK		(	measured_timestamp > TIMESTAMP '2014-01-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-02-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201401 ON bluetooth.observations_201401 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201402 (
	CONSTRAINT	pk_201402	primary key (id),
	CONSTRAINT	ck_201402	CHECK		(	measured_timestamp > TIMESTAMP '2014-02-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-03-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201402 ON bluetooth.observations_201402 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201403 (
	CONSTRAINT	pk_201403	primary key (id),
	CONSTRAINT	ck_201403	CHECK		(	measured_timestamp > TIMESTAMP '2014-03-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-04-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201403 ON bluetooth.observations_201403 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201404 (
	CONSTRAINT	pk_201404	primary key (id),
	CONSTRAINT	ck_201404	CHECK		(	measured_timestamp > TIMESTAMP '2014-04-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-05-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201404 ON bluetooth.observations_201404 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201405 (
	CONSTRAINT	pk_201405	primary key (id),
	CONSTRAINT	ck_201405	CHECK		(	measured_timestamp > TIMESTAMP '2014-05-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-06-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201405 ON bluetooth.observations_201405 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201406 (
	CONSTRAINT	pk_201406	primary key (id),
	CONSTRAINT	ck_201406	CHECK		(	measured_timestamp > TIMESTAMP '2014-06-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-07-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201406 ON bluetooth.observations_201406 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201407 (
	CONSTRAINT	pk_201407	primary key (id),
	CONSTRAINT	ck_201407	CHECK		(	measured_timestamp > TIMESTAMP '2014-07-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-08-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201407 ON bluetooth.observations_201407 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201408 (
	CONSTRAINT	pk_201408	primary key (id),
	CONSTRAINT	ck_201408	CHECK		(	measured_timestamp > TIMESTAMP '2014-08-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-09-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201408 ON bluetooth.observations_201408 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201409 (
	CONSTRAINT	pk_201409	primary key (id),
	CONSTRAINT	ck_201409	CHECK		(	measured_timestamp > TIMESTAMP '2014-09-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-10-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201409 ON bluetooth.observations_201409 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201410 (
	CONSTRAINT	pk_201410	primary key (id),
	CONSTRAINT	ck_201410	CHECK		(	measured_timestamp > TIMESTAMP '2014-10-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-11-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201410 ON bluetooth.observations_201410 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201411 (
	CONSTRAINT	pk_201411	primary key (id),
	CONSTRAINT	ck_201411	CHECK		(	measured_timestamp > TIMESTAMP '2014-11-01' 
							AND 	measured_timestamp <= TIMESTAMP '2014-12-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201411 ON bluetooth.observations_201411 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201412 (
	CONSTRAINT	pk_201412	primary key (id),
	CONSTRAINT	ck_201412	CHECK		(	measured_timestamp > TIMESTAMP '2014-12-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-01-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201412 ON bluetooth.observations_201412 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201501 (
	CONSTRAINT	pk_201501	primary key (id),
	CONSTRAINT	ck_201501	CHECK		(	measured_timestamp > TIMESTAMP '2015-01-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-02-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201501 ON bluetooth.observations_201501 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201502 (
	CONSTRAINT	pk_201502	primary key (id),
	CONSTRAINT	ck_201502	CHECK		(	measured_timestamp > TIMESTAMP '2015-02-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-03-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201502 ON bluetooth.observations_201502 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201503 (
	CONSTRAINT	pk_201503	primary key (id),
	CONSTRAINT	ck_201503	CHECK		(	measured_timestamp > TIMESTAMP '2015-03-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-04-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201503 ON bluetooth.observations_201503 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201504 (
	CONSTRAINT	pk_201504	primary key (id),
	CONSTRAINT	ck_201504	CHECK		(	measured_timestamp > TIMESTAMP '2015-04-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-05-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201504 ON bluetooth.observations_201504 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201505 (
	CONSTRAINT	pk_201505	primary key (id),
	CONSTRAINT	ck_201505	CHECK		(	measured_timestamp > TIMESTAMP '2015-05-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-06-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201505 ON bluetooth.observations_201505 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201506 (
	CONSTRAINT	pk_201506	primary key (id),
	CONSTRAINT	ck_201506	CHECK		(	measured_timestamp > TIMESTAMP '2015-06-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-07-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201506 ON bluetooth.observations_201506 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201507 (
	CONSTRAINT	pk_201507	primary key (id),
	CONSTRAINT	ck_201507	CHECK		(	measured_timestamp > TIMESTAMP '2015-07-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-08-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201507 ON bluetooth.observations_201507 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201508 (
	CONSTRAINT	pk_201508	primary key (id),
	CONSTRAINT	ck_201508	CHECK		(	measured_timestamp > TIMESTAMP '2015-08-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-09-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201508 ON bluetooth.observations_201508 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201509 (
	CONSTRAINT	pk_201509	primary key (id),
	CONSTRAINT	ck_201509	CHECK		(	measured_timestamp > TIMESTAMP '2015-09-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-10-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201509 ON bluetooth.observations_201509 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201510 (
	CONSTRAINT	pk_201510	primary key (id),
	CONSTRAINT	ck_201510	CHECK		(	measured_timestamp > TIMESTAMP '2015-10-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-11-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201510 ON bluetooth.observations_201510 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201511 (
	CONSTRAINT	pk_201511	primary key (id),
	CONSTRAINT	ck_201511	CHECK		(	measured_timestamp > TIMESTAMP '2015-11-01' 
							AND 	measured_timestamp <= TIMESTAMP '2015-12-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201511 ON bluetooth.observations_201511 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201512 (
	CONSTRAINT	pk_201512	primary key (id),
	CONSTRAINT	ck_201512	CHECK		(	measured_timestamp > TIMESTAMP '2015-12-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-01-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201512 ON bluetooth.observations_201512 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201601 (
	CONSTRAINT	pk_201601	primary key (id),
	CONSTRAINT	ck_201601	CHECK		(	measured_timestamp > TIMESTAMP '2016-01-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-02-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201601 ON bluetooth.observations_201601 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201602 (
	CONSTRAINT	pk_201602	primary key (id),
	CONSTRAINT	ck_201602	CHECK		(	measured_timestamp > TIMESTAMP '2016-02-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-03-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201602 ON bluetooth.observations_201602 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201603 (
	CONSTRAINT	pk_201603	primary key (id),
	CONSTRAINT	ck_201603	CHECK		(	measured_timestamp > TIMESTAMP '2016-03-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-04-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201603 ON bluetooth.observations_201603 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201604 (
	CONSTRAINT	pk_201604	primary key (id),
	CONSTRAINT	ck_201604	CHECK		(	measured_timestamp > TIMESTAMP '2016-04-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-05-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201604 ON bluetooth.observations_201604 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201605 (
	CONSTRAINT	pk_201605	primary key (id),
	CONSTRAINT	ck_201605	CHECK		(	measured_timestamp > TIMESTAMP '2016-05-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-06-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201605 ON bluetooth.observations_201605 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201606 (
	CONSTRAINT	pk_201606	primary key (id),
	CONSTRAINT	ck_201606	CHECK		(	measured_timestamp > TIMESTAMP '2016-06-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-07-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201606 ON bluetooth.observations_201606 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201607 (
	CONSTRAINT	pk_201607	primary key (id),
	CONSTRAINT	ck_201607	CHECK		(	measured_timestamp > TIMESTAMP '2016-07-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-08-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201607 ON bluetooth.observations_201607 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201608 (
	CONSTRAINT	pk_201608	primary key (id),
	CONSTRAINT	ck_201608	CHECK		(	measured_timestamp > TIMESTAMP '2016-08-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-09-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201608 ON bluetooth.observations_201608 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201609 (
	CONSTRAINT	pk_201609	primary key (id),
	CONSTRAINT	ck_201609	CHECK		(	measured_timestamp > TIMESTAMP '2016-09-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-10-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201609 ON bluetooth.observations_201609 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201610 (
	CONSTRAINT	pk_201610	primary key (id),
	CONSTRAINT	ck_201610	CHECK		(	measured_timestamp > TIMESTAMP '2016-10-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-11-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201610 ON bluetooth.observations_201610 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201611 (
	CONSTRAINT	pk_201611	primary key (id),
	CONSTRAINT	ck_201611	CHECK		(	measured_timestamp > TIMESTAMP '2016-11-01' 
							AND 	measured_timestamp <= TIMESTAMP '2016-12-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201611 ON bluetooth.observations_201611 (analysis_id);

CREATE TABLE IF NOT EXISTS bluetooth.observations_201612 (
	CONSTRAINT	pk_201612	primary key (id),
	CONSTRAINT	ck_201612	CHECK		(	measured_timestamp > TIMESTAMP '2016-12-01' 
							AND 	measured_timestamp <= TIMESTAMP '2017-01-01')
	) INHERITS (bluetooth.observations);
CREATE INDEX idx_ai_201612 ON bluetooth.observations_201612 (analysis_id);