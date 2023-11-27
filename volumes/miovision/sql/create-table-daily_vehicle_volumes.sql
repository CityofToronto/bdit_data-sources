CREATE TABLE miovision_api.daily_vehicle_volumes(
    intersection_uid integer,
    dt date,
    vehicle_volume integer,
    isodow smallint,
    holiday smallint,
    unacceptable_gap_minutes smallint,
    datetime_bins_missing smallint,
    CONSTRAINT daily_vehicle_volumes_pkey
    PRIMARY KEY (intersection_uid, dt)
); 

ALTER TABLE miovision_api.daily_vehicle_volumes OWNER TO miovision_admins;
GRANT ALL ON TABLE miovision_api.daily_vehicle_volumes TO dbadmin;
GRANT SELECT ON TABLE miovision_api.daily_vehicle_volumes TO bdit_humans;
GRANT SELECT, INSERT, DELETE ON TABLE miovision_api.daily_vehicle_volumes TO miovision_api_bot;

COMMENT ON TABLE miovision_api.daily_vehicle_volumes
IS '''Daily vehicle volumes. Excludes `anomalous_ranges` (use discouraged based on investigations)
but does not exclude time around `unacceptable_gaps` (zero volume periods).''';

COMMENT ON COLUMN miovision_api.daily_vehicle_volumes.isodow
IS 'Use `WHERE iso dow <= 5 AND holiday = 0` for non-holiday weekdays.';

COMMENT ON COLUMN miovision_api.daily_vehicle_volumes.unacceptable_gap_minutes
IS 'Periods of consecutive zero volumes deemed unacceptable based on avg intersection volume in that hour.';

COMMENT ON COLUMN miovision_api.daily_vehicle_volumes.datetime_bins_missing
IS 'Minutes with zero vehicle volumes out of a total of possible 1440 minutes.';