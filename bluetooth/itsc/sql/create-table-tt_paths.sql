DROP TABLE bluetooth.itsc_tt_paths;
CREATE TABLE bluetooth.itsc_tt_paths (
    division_id smallint,
    path_id integer,
    source_id character varying(1000),
    algorithm integer,
    first_feature_start_off_set_meters double precision,
    last_feature_end_off_set_meters double precision,
    first_feature_forward boolean,
    raw_data_types integer,
    external_data_origin character varying(400),
    external_data_destination character varying(400),
    external_data_waypoints character varying(10000),
    time_adjust_factor double precision,
    time_adjust_constant_seconds double precision,
    queue_max_speed_kmh double precision,
    minimal_delay_speed_kmh double precision,
    major_delay_speed_kmh double precision,
    severe_delay_speed_kmh double precision,
    use_minimum_speed boolean,
    length_m double precision,
    start_timestamp timestamp without time zone,
    end_timestamp timestamp without time zone,
    featurespeed_division_id integer,
    severe_delay_issue_division_id smallint,
    major_delay_issue_division_id smallint,
    minor_delay_issue_division_id smallint,
    queue_issue_division_id smallint,
    queue_detection_clearance_speed_kmh double precision,
    severe_delay_clearance_speed_kmh double precision,
    path_type smallint,
    path_data_timeout_for_issue_creation_seconds integer,
    encoded_polyline character varying,
    centreline_ids bigint [],
    geom geometry,
    CONSTRAINT tt_paths_pkey PRIMARY KEY (
        division_id,
        path_id,
        start_timestamp
    )
);

ALTER TABLE bluetooth.itsc_tt_paths OWNER TO bt_admins;

CREATE TRIGGER add_bluetooth_path_geom_trigger
BEFORE INSERT OR UPDATE ON bluetooth.itsc_tt_paths
FOR EACH ROW
EXECUTE FUNCTION bluetooth.itsc_add_bluetooth_path_geom();

GRANT SELECT, UPDATE, INSERT ON bluetooth.itsc_tt_paths TO events_bot;
