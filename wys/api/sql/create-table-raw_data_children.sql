-- Table: wys.raw_data_2017

-- DROP TABLE wys.raw_data_2017;

CREATE TABLE wys.raw_data_2017
(
	PRIMARY KEY (raw_data_uid),
    CONSTRAINT raw_data_2017_api_id_datetime_bin_speed_key UNIQUE (api_id, datetime_bin, speed),
	CHECK (datetime_bin >= '2017-01-01' AND datetime_bin < '2017-01-01'::DATE + INTERVAL '1 Year')
)
INHERITS(wys.raw_data) 
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE wys.raw_data_2017
    OWNER to rdumas;

GRANT SELECT, UPDATE, INSERT ON TABLE wys.raw_data_2017 TO wys_bot;
-- Index: raw_data_2017_datetime_bin_idx

-- DROP INDEX wys.raw_data_2017_datetime_bin_idx;

CREATE INDEX raw_data_2017_datetime_bin_idx
    ON wys.raw_data_2017 USING brin
    (datetime_bin)
    TABLESPACE pg_default;
	
CREATE TABLE wys.raw_data_2018
(
	PRIMARY KEY (raw_data_uid),
    CONSTRAINT raw_data_2018_api_id_datetime_bin_speed_key UNIQUE (api_id, datetime_bin, speed),
	CHECK (datetime_bin >= '2018-01-01' AND datetime_bin < '2018-01-01'::DATE + INTERVAL '1 Year')
)
INHERITS(wys.raw_data) 
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE wys.raw_data_2018
    OWNER to rdumas;

GRANT SELECT, UPDATE, INSERT ON TABLE wys.raw_data_2018 TO wys_bot;
-- Index: raw_data_2018_datetime_bin_idx

-- DROP INDEX wys.raw_data_2018_datetime_bin_idx;

CREATE INDEX raw_data_2018_datetime_bin_idx
    ON wys.raw_data_2018 USING brin
    (datetime_bin)
    TABLESPACE pg_default;
	
CREATE TABLE wys.raw_data_2019
(
	PRIMARY KEY (raw_data_uid),
    CONSTRAINT raw_data_2019_api_id_datetime_bin_speed_key UNIQUE (api_id, datetime_bin, speed),
	CHECK (datetime_bin >= '2019-01-01' AND datetime_bin < '2019-01-01'::DATE + INTERVAL '1 Year')
)
INHERITS(wys.raw_data) 
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE wys.raw_data_2019
    OWNER to rdumas;

GRANT SELECT, UPDATE, INSERT ON TABLE wys.raw_data_2019 TO wys_bot;
-- Index: raw_data_2019_datetime_bin_idx

-- DROP INDEX wys.raw_data_2019_datetime_bin_idx;

CREATE INDEX raw_data_2019_datetime_bin_idx
    ON wys.raw_data_2019 USING brin
    (datetime_bin)
    TABLESPACE pg_default;
	
CREATE TABLE wys.raw_data_2020
(
	PRIMARY KEY (raw_data_uid),
    CONSTRAINT raw_data_2020_api_id_datetime_bin_speed_key UNIQUE (api_id, datetime_bin, speed),
	CHECK (datetime_bin >= '2020-01-01' AND datetime_bin < '2020-01-01'::DATE + INTERVAL '1 Year')
)
INHERITS(wys.raw_data) 
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE wys.raw_data_2020
    OWNER to rdumas;

GRANT SELECT, UPDATE, INSERT ON TABLE wys.raw_data_2020 TO wys_bot;
-- Index: raw_data_2020_datetime_bin_idx

-- DROP INDEX wys.raw_data_2020_datetime_bin_idx;

CREATE INDEX raw_data_2020_datetime_bin_idx
    ON wys.raw_data_2020 USING brin
    (datetime_bin)
    TABLESPACE pg_default;