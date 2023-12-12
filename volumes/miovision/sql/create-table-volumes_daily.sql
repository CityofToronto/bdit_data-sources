CREATE TABLE miovision_api.volumes_daily(
    dt date NOT NULL,
    intersection_uid integer NOT NULL,
    classification_uid integer NOT NULL,
    daily_volume int,
    isodow smallint NOT NULL,
    holiday boolean NOT NULL,
    datetime_bins_missing smallint,
    unacceptable_gap_minutes smallint,
    avg_historical_gap_vol int
    CONSTRAINT volumes_daily_pkey
    PRIMARY KEY (intersection_uid, dt, classification_uid)
); 

CREATE INDEX volumes_intersection_idx
ON miovision_api.volumes_daily
USING btree(intersection_uid);

CREATE INDEX volumes_dt_idx
ON miovision_api.volumes_daily
USING btree(dt);

ALTER TABLE miovision_api.volumes_daily OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.volumes_daily TO bdit_humans;
GRANT SELECT, INSERT, DELETE ON TABLE miovision_api.volumes_daily TO miovision_api_bot;

COMMENT ON TABLE miovision_api.volumes_daily
IS '''Daily volumes by intersection_uid, classification_uid.
Excludes `anomalous_ranges` (use discouraged based on investigations)
but does not exclude time around `unacceptable_gaps` (zero volume periods).''';

COMMENT ON COLUMN miovision_api.volumes_daily.isodow
IS 'Use `WHERE isodow <= 5 AND holiday is False` for non-holiday weekdays.';

COMMENT ON COLUMN miovision_api.volumes_daily.unacceptable_gap_minutes
IS 'Periods of consecutive zero volumes deemed unacceptable
based on avg intersection volume in that hour.';

COMMENT ON COLUMN miovision_api.volumes_daily.datetime_bins_missing
IS 'Minutes with zero vehicle volumes out of a total of possible 1440 minutes.';

COMMENT ON COLUMN miovision_api.volumes_daily.avg_historical_gap_vol
IS 'Avg historical volume for that classification and gap duration 
based on averages from a 60 day lookback in that hour.';
