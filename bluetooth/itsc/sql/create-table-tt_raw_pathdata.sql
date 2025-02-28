DROP TABLE IF EXISTS gwolofs.tt_raw_pathdata;
CREATE TABLE gwolofs.tt_raw_pathdata (
    division_id smallint,
    path_id integer,
    dt timestamp without time zone,
    travel_time_s integer,
    quality_metric double precision,
    num_samples integer,
    congestion_start_meters double precision,
    congestion_end_meters double precision,
    min_speed_kmh double precision,
    unmatched integer,
    fifth_percentile_tt_s integer,
    ninty_fifth_percentile_tt_s integer
);

CREATE UNIQUE INDEX tt_raw_pathdata_path_dt_idx
ON gwolofs.tt_raw_pathdata USING btree(
    path_id, dt
);

ALTER TABLE gwolofs.tt_raw_pathdata ADD CONSTRAINT
tt_raw_pathdata_path_dt_pkey PRIMARY KEY (path_id, dt);

ALTER TABLE gwolofs.tt_raw_pathdata OWNER TO gwolofs;
GRANT ALL ON gwolofs.tt_raw_pathdata TO events_bot;
