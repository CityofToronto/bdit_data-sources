CREATE TABLE IF NOT EXISTS bluetooth.tpana_links (
    link_id varchar PRIMARY KEY,
    link_name varchar,
    short_name varchar,
    additional_info text,
    src_detector_id varchar REFERENCES bluetooth.tpana_detectors (detector_id),
    dest_detector_id varchar REFERENCES bluetooth.tpana_detectors (detector_id),
    line_distance_m float,
    path_distance_m float,
    route_direction_name varchar,
    speed_limit_kmh int,
    max_travel_time_s int,
    detection_rules varchar,
    real_time_stats varchar,
    comments text,
    extra_info text
);

COMMENT ON TABLE bluetooth.tpana_links
IS 'Link details extracted from TPANA''s Equipment.xml received on 2026-06-02.';

ALTER TABLE bluetooth.tpana_links OWNER TO bt_admins;

GRANT SELECT ON TABLE bluetooth.tpana_links TO bdit_humans;
