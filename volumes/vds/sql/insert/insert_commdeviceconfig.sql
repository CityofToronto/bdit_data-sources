INSERT INTO vds.config_comms_device AS e (
    division_id, fss_id, source_id, start_timestamp,
    end_timestamp, has_gps_unit, device_type, description
)
VALUES %s
ON CONFLICT (division_id, fss_id, start_timestamp)
DO UPDATE
SET end_timestamp = EXCLUDED.end_timestamp
WHERE
    e.division_id = EXCLUDED.division_id
    AND e.fss_id = EXCLUDED.fss_id
    AND e.start_timestamp = EXCLUDED.start_timestamp;