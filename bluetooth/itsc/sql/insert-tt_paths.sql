INSERT INTO gwolofs.tt_paths (
    division_id, path_id, source_id, algorithm, first_feature_start_off_set_meters,
    last_feature_end_off_set_meters, first_feature_forward, raw_data_types, external_data_origin,
    external_data_destination, external_data_waypoints, time_adjust_factor,
    time_adjust_constant_seconds, queue_max_speed_kmh, minimal_delay_speed_kmh,
    major_delay_speed_kmh, severe_delay_speed_kmh, use_minimum_speed, length_m, start_timestamp,
    end_timestamp, featurespeed_division_id, severe_delay_issue_division_id,
    major_delay_issue_division_id, minor_delay_issue_division_id, queue_issue_division_id,
    queue_detection_clearance_speed_kmh, severe_delay_clearance_speed_kmh, path_type,
    path_data_timeout_for_issue_creation_seconds, encoded_polyline
)
VALUES %s
/*ON CONFLICT (divisionid, issueid)
DO UPDATE SET
*/