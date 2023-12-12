CREATE TABLE miovision_api.daily_volumes(
    dt date,
    intersection_uid integer,
    classification_uid integer,
    daily_volume int,
    isodow smallint,
    holiday boolean,
    datetime_bins_missing smallint,
    unacceptable_gap_minutes smallint,
    avg_historical_gap_vol int
    CONSTRAINT daily_volumes_pkey
    PRIMARY KEY (intersection_uid, dt, classification_uid)
); 

ALTER TABLE miovision_api.daily_volumes OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.daily_volumes TO bdit_humans;
GRANT SELECT, INSERT, DELETE ON TABLE miovision_api.daily_volumes TO miovision_api_bot;

COMMENT ON TABLE miovision_api.daily_volumes
IS '''Daily volumes by intersection_uid, classification_uid.
Excludes `anomalous_ranges` (use discouraged based on investigations)
but does not exclude time around `unacceptable_gaps` (zero volume periods).''';

COMMENT ON COLUMN miovision_api.daily_volumes.isodow
IS 'Use `WHERE isodow <= 5 AND holiday is False` for non-holiday weekdays.';

COMMENT ON COLUMN miovision_api.daily_volumes.unacceptable_gap_minutes
IS 'Periods of consecutive zero volumes deemed unacceptable
based on avg intersection volume in that hour.';

COMMENT ON COLUMN miovision_api.daily_volumes.datetime_bins_missing
IS 'Minutes with zero vehicle volumes out of a total of possible 1440 minutes.';

COMMENT ON COLUMN miovision_api.daily_volumes.avg_historical_gap_vol
IS 'Avg historical volume for that classification and gap duration 
based on averages from a 60 day lookback in that hour.';
