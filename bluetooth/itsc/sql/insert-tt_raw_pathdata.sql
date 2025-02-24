INSERT INTO gwolofs.tt_raw_pathdata (
    division_id, path_id, dt, travel_time_s, quality_metric, congestion_start_landmark_index,
    congestion_end_landmark_index, num_samples, congestion_start_meters, congestion_end_meters,
    min_speed_kmh, unmatched, fifth_percentile_travel_time_s, ninty_fifth_percentile_travel_time_s
)
VALUES %s
--ON CONFLICT (division_id, path_id, dt) DO NOTHING;
