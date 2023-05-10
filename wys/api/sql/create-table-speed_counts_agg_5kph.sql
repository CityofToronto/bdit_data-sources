-- Table: wys.speed_counts_agg_5kph

-- DROP TABLE wys.speed_counts_agg_5kph;

CREATE TABLE IF NOT EXISTS wys.speed_counts_agg_5kph
(
    speed_counts_agg_5kph_id bigint NOT NULL DEFAULT nextval('wys.speed_counts_agg_5kph_speed_counts_agg_5kph_id_seq'::regclass),
    api_id integer NOT NULL,
    datetime_bin timestamp without time zone NOT NULL,
    speed_id integer NOT NULL,
    volume integer NOT NULL,
    CONSTRAINT speed_counts_agg_5kph_pkey PRIMARY KEY (speed_counts_agg_5kph_id),
    CONSTRAINT speed_counts_agg_5kph_api_id_datetime_bin_speed_id_key UNIQUE (api_id, datetime_bin, speed_id)
);

ALTER TABLE wys.speed_counts_agg_5kph
    OWNER to wys_admins;

REVOKE ALL ON TABLE wys.speed_counts_agg_5kph FROM wys_bot;

GRANT REFERENCES, SELECT, TRIGGER ON TABLE wys.speed_counts_agg_5kph TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE wys.speed_counts_agg_5kph TO rds_superuser WITH GRANT OPTION;

GRANT SELECT, UPDATE, INSERT ON TABLE wys.speed_counts_agg_5kph TO wys_bot;

COMMENT ON TABLE wys.speed_counts_agg_5kph
    IS '5kph one hour aggregation of speeeeeeeeeeedz';
-- Index: speed_counts_agg_5kph_api_id_idx

-- DROP INDEX wys.speed_counts_agg_5kph_api_id_idx;

CREATE INDEX speed_counts_agg_5kph_api_id_idx
    ON wys.speed_counts_agg_5kph USING btree
    (api_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: speed_counts_agg_5kph_datetime_bin_idx

-- DROP INDEX wys.speed_counts_agg_5kph_datetime_bin_idx;

CREATE INDEX speed_counts_agg_5kph_datetime_bin_idx
    ON wys.speed_counts_agg_5kph USING brin
    (datetime_bin)
    TABLESPACE pg_default;
-- Index: speed_counts_agg_5kph_speed_id_idx

-- DROP INDEX wys.speed_counts_agg_5kph_speed_id_idx;

CREATE INDEX speed_counts_agg_5kph_speed_id_idx
    ON wys.speed_counts_agg_5kph USING btree
    (speed_id ASC NULLS LAST)
    TABLESPACE pg_default;