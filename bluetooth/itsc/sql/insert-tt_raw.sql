TRUNCATE gwolofs.tt_raw;
INSERT INTO gwolofs.tt_raw (
    division_id, path_id, raw_data_type, dt, travel_time_s, quality_metric, num_samples,
    congestion_start_meters, congestion_end_meters, min_speed_kmh, fifth_percentile_tt_s,
    nintyfifth_percentile_tt_s, unmatched 
)
VALUES %s;
