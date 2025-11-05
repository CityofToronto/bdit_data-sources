INSERT INTO bluetooth.itsc_tt_raw_pathdata (
    division_id, path_id, dt, travel_time_s, quality_metric, num_samples, congestion_start_meters,
    congestion_end_meters, min_speed_kmh, unmatched, fifth_percentile_tt_s,
    ninty_fifth_percentile_tt_s
)
VALUES %s
--ON CONFLICT (division_id, path_id, dt) DO NOTHING;
