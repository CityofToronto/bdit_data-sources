-- Table: gwolofs.congestion_raw_corridors

-- DROP TABLE IF EXISTS gwolofs.congestion_raw_corridors;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_raw_corridors
(
    corridor_id smallint,
    time_grp timerange NOT NULL,
    bin_range tsrange NOT NULL,
    tt real,
    num_obs integer,
    uri_string text COLLATE pg_catalog."default",
    dt date,
    hr smallint,
    CONSTRAINT congestion_raw_corridors_pkey PRIMARY KEY (corridor_id, bin_range, time_grp),
    CONSTRAINT corridor_fkey FOREIGN KEY (corridor_id)
    REFERENCES gwolofs.congestion_corridors (corridor_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE
    NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_raw_corridors
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_raw_corridors FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.congestion_raw_corridors TO bdit_humans;

GRANT ALL ON TABLE gwolofs.congestion_raw_corridors TO gwolofs;

-- Index: congestion_raw_corridors_dt_idx

-- DROP INDEX IF EXISTS gwolofs.congestion_raw_corridors_dt_idx;

CREATE INDEX IF NOT EXISTS congestion_raw_corridors_dt_idx
ON gwolofs.congestion_raw_corridors USING brin
(dt)
TABLESPACE pg_default;
-- Index: congestion_raw_corridors_uri_string

-- DROP INDEX IF EXISTS gwolofs.congestion_raw_corridors_uri_string;

CREATE INDEX IF NOT EXISTS congestion_raw_corridors_uri_string
ON gwolofs.congestion_raw_corridors USING btree
(uri_string COLLATE pg_catalog."default" ASC NULLS LAST)
WITH (deduplicate_items = TRUE)
TABLESPACE pg_default;
-- Index: dynamic_binning_results_time_grp_corridor_id_idx

-- DROP INDEX IF EXISTS gwolofs.dynamic_binning_results_time_grp_corridor_id_idx;

CREATE INDEX IF NOT EXISTS dynamic_binning_results_time_grp_corridor_id_idx
ON gwolofs.congestion_raw_corridors USING btree
(time_grp ASC NULLS LAST, corridor_id ASC NULLS LAST, dt ASC NULLS LAST)
WITH (deduplicate_items = TRUE)
TABLESPACE pg_default;

COMMENT ON TABLE gwolofs.congestion_raw_corridors IS
'Stores dynamic binning results for custom corridor based travel time requests.';

COMMENT ON TABLE gwolofs.congestion_raw_corridors
IS 'Stores dynamic binning results from standard HERE congestion network travel time aggregations.';

COMMENT ON COLUMN gwolofs.congestion_raw_corridors.bin_range
IS 'Bin range. An exclusion constraint on a temp table prevents overlapping ranges during insert.';

COMMENT ON COLUMN gwolofs.congestion_raw_corridors.tt
IS 'Travel time in seconds.';

COMMENT ON COLUMN gwolofs.congestion_raw_corridors.num_obs
IS 'The sum of the sample size from here.ta_path.';

COMMENT ON COLUMN gwolofs.congestion_raw_corridors.dt
IS 'The date of aggregation for the record. Records may not overlap dates.';

COMMENT ON COLUMN gwolofs.congestion_raw_corridors.hr
IS 'The hour the majority of the record occured in. Ties are rounded up.';
