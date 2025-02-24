DROP TABLE gwolofs.tt_raw;
CREATE TABLE gwolofs.tt_raw (
    division_id smallint,
    path_id integer,
    raw_data_type integer,
    dt timestamp without time zone,
    travel_time_s integer,
    quality_metric double precision,
    num_samples integer,
    congestion_start_meters double precision,
    congestion_end_meters double precision,
    min_speed_kmh double precision,
    fifth_percentile_tt_s integer,
    nintyfifth_percentile_tt_s integer,
    unmatched integer
);

ALTER TABLE gwolofs.tt_raw OWNER TO gwolofs;
GRANT ALL ON gwolofs.tt_raw TO events_bot;
