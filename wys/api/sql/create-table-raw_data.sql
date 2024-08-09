-- Table: wys.raw_data

-- DROP TABLE wys.raw_data;

CREATE TABLE wys.raw_data(
    raw_data_uid bigint NOT NULL DEFAULT nextval('wys_raw_data_raw_data_uid_seq'::regclass),
    api_id integer NOT NULL,
    datetime_bin timestamp without time zone NOT NULL,
    speed integer,
    count integer,
    speed_count_uid integer,
    CONSTRAINT wys_raw_data_pkey
    PRIMARY KEY (datetime_bin, raw_data_uid),
    CONSTRAINT raw_data_api_id_datetime_bin_speed_key
    UNIQUE NULLS NOT DISTINCT (datetime_bin, api_id, speed)
)
PARTITION BY RANGE (datetime_bin)
WITH (OIDS=FALSE);

CREATE INDEX IF NOT EXISTS raw_data_datetime_bin_idx
ON wys.raw_data USING brin(datetime_bin);

ALTER TABLE wys.raw_data OWNER TO wys_admins;
GRANT SELECT, REFERENCES ON TABLE wys.raw_data TO bdit_humans WITH GRANT OPTION;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE wys.raw_data TO wys_bot;

GRANT ALL ON SEQUENCE wys.wys_raw_data_raw_data_uid_seq TO wys_bot;
--annual partitions created with `wys.create_yyyy_raw_data_partition`.
--monthly partitions created with `wys.create_mm_nested_raw_data_partitions`.
