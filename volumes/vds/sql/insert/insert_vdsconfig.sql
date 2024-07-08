INSERT INTO vds.vdsconfig AS c (
    division_id, vds_id, detector_id, start_timestamp, end_timestamp, lanes, has_gps_unit, 
    management_url, description, fss_division_id, fss_id, rtms_from_zone, rtms_to_zone,
    detector_type, created_by, created_by_staffid, signal_id, signal_division_id, movement)
VALUES %s
ON CONFLICT (division_id, vds_id, start_timestamp)
DO UPDATE
SET end_timestamp = EXCLUDED.end_timestamp
WHERE
    c.division_id = EXCLUDED.division_id
    AND c.vds_id = EXCLUDED.vds_id
    AND c.start_timestamp = EXCLUDED.start_timestamp;