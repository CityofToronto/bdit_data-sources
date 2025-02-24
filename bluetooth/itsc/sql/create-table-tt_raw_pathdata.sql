DROP TABLE gwolofs.tt_raw_pathdata;
CREATE TABLE gwolofs.tt_raw_pathdata (
    division_id smallint
    path_id integer
    dt timestamp without time zone
    travel_time_s integer
    quality_metric double precision
    congestion_start_landmark_index integer
    congestion_end_landmark_index integer
    num_samples integer
    congestion_start_meters double precision
    congestion_end_meters double precision
    min_speed_kmh double precision
    unmatched integer
    fifth_percentile_travel_time_s integer
    ninty_fifth_percentile_travel_time_s integer
);

ALTER TABLE gwolofs.tt_raw_pathdata OWNER TO gwolofs;
GRANT ALL ON gwolofs.tt_raw_pathdata TO events_bot;
