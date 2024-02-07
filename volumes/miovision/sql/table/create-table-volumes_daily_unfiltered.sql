CREATE TABLE miovision_api.volumes_daily_unfiltered (
    dt date NOT NULL,
    intersection_uid integer NOT NULL,
    classification_uid integer NOT NULL,
    daily_volume int,
    isodow smallint NOT NULL,
    holiday boolean NOT NULL,
    datetime_bins_missing smallint,
    unacceptable_gap_minutes smallint,
    avg_historical_gap_vol int,
    CONSTRAINT volumes_daily_pkey
    PRIMARY KEY (intersection_uid, dt, classification_uid)
);

CREATE INDEX volumes_intersection_idx
ON miovision_api.volumes_daily_unfiltered
USING btree (intersection_uid);

CREATE INDEX volumes_dt_idx
ON miovision_api.volumes_daily_unfiltered
USING btree (dt);

ALTER TABLE miovision_api.volumes_daily_unfiltered OWNER TO miovision_admins;
GRANT SELECT, INSERT, DELETE ON TABLE miovision_api.volumes_daily_unfiltered TO miovision_api_bot;
REVOKE ALL ON TABLE miovision_api.volumes_daily_unfiltered FROM bdit_humans;
GRANT SELECT ON TABLE miovision_api.volumes_daily_unfiltered TO miovision_data_detectives;

COMMENT ON TABLE miovision_api.volumes_daily_unfiltered
IS '''Daily volumes by intersection_uid, classification_uid.
!!! Does not exclude `anomalous_ranges` - use VIEW `volumes_daily` for most purposes instead. 
but does not exclude time around `unacceptable_gaps` (zero volume periods).''';

COMMENT ON COLUMN miovision_api.volumes_daily_unfiltered.isodow
IS 'Use `WHERE isodow <= 5 AND holiday is False` for non-holiday weekdays.';

COMMENT ON COLUMN miovision_api.volumes_daily_unfiltered.unacceptable_gap_minutes
IS 'Consecutive minutes with zero volume deemed unacceptable for use based on a
threshold set using avg historical intersection volume in the same hour.';

COMMENT ON COLUMN miovision_api.volumes_daily_unfiltered.datetime_bins_missing
IS 'Minutes with zero vehicle volumes out of a total of possible 1440 minutes.';

COMMENT ON COLUMN miovision_api.volumes_daily_unfiltered.avg_historical_gap_vol
IS 'Avg historical volume for that classification and gap duration 
based on averages from a 60 day lookback in that hour.';