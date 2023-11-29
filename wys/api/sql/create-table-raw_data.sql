-- Table: wys.raw_data

-- DROP TABLE wys.raw_data;

CREATE TABLE wys.raw_data (
    raw_data_uid bigint NOT NULL DEFAULT nextval('wys_raw_data_raw_data_uid_seq'::regclass),
    api_id integer NOT NULL,
    datetime_bin timestamp without time zone NOT NULL,
    speed integer NOT NULL,
    count integer,
    speed_count_uid integer,
    CONSTRAINT wys_raw_data_pkey PRIMARY KEY (api_id, datetime_bin, speed)
)
PARTITION BY RANGE (datetime_bin)
WITH (
    OIDS=FALSE
);

CREATE INDEX IF NOT EXISTS raw_data_datetime_bin_idx
ON wys.raw_data USING brin(datetime_bin);

ALTER TABLE wys.raw_data OWNER TO wys_admins;
GRANT SELECT, REFERENCES ON TABLE wys.raw_data TO bdit_humans WITH GRANT OPTION;