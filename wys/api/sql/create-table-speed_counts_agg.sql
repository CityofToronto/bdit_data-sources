-- Table: wys.speed_counts_agg

-- DROP TABLE wys.speed_counts_agg;

CREATE TABLE wys.speed_counts_agg_5kph
(
    speed_counts_agg_id bigserial,
    api_id integer,
    datetime_bin timestamp without time zone,
    speed_id integer,
    volume integer,
    CONSTRAINT speed_counts_agg_pkey PRIMARY KEY (speed_counts_agg_id),
    CONSTRAINT speed_counts_agg_api_id_datetime_bin_speed_id_key UNIQUE (api_id, datetime_bin, speed_id)
)
WITH (
    OIDS=FALSE
);
ALTER TABLE wys.speed_counts_agg_5kph OWNER TO wys_admins;
GRANT ALL ON TABLE wys.speed_counts_agg_5kph TO rds_superuser;
GRANT SELECT ON TABLE wys.speed_counts_agg_5kph TO bdit_humans WITH GRANT OPTION;
GRANT SELECT, UPDATE, INSERT ON TABLE wys.speed_counts_agg_5kph TO wys_bot;
