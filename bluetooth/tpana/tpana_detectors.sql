CREATE TABLE IF NOT EXISTS bluetooth.tpana_detectors (
    detector_id varchar PRIMARY KEY,
    detector_name varchar,
    short_name varchar,
    equipment_type varchar,
    latitude float,
    longitude float,
    altitude float,
    utc_offset int,
    allowed_silence_s int,
    gapout_time_s int,
    real_time_stats int,
    battery_alarm_threshold_v float,
    additional_info text,
    comments text
);

COMMENT ON TABLE bluetooth.tpana_detectors
IS 'Detector details extracted from TPANA''s Equipment.xml received on 2026-06-02.';

ALTER TABLE bluetooth.tpana_detectors OWNER TO bt_admins;

GRANT SELECT ON TABLE bluetooth.tpana_detectors TO bdit_humans;

ALTER TABLE bluetooth.tpana_detectors ADD COLUMN geom geometry;
UPDATE bluetooth.tpana_detectors SET geom = st_setsrid(st_makepoint(longitude, latitude), 4326);
