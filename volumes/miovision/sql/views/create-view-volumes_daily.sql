CREATE VIEW miovision_api.volumes_daily AS (
    SELECT
        v.dt,
        v.intersection_uid,
        v.classification_uid,
        v.daily_volume,
        v.isodow,
        v.holiday,
        v.datetime_bins_missing,
        v.unacceptable_gap_minutes,
        v.avg_historical_gap_vol
    FROM miovision_api.volumes_daily_unfiltered AS v
    --anti join anomalous_ranges VIEW
    LEFT JOIN miovision_api.anomalous_ranges AS ar ON
        (
            ar.intersection_uid = v.intersection_uid
            OR ar.intersection_uid IS NULL
        ) AND (
            ar.classification_uid = v.classification_uid
            OR ar.classification_uid IS NULL
        )
        AND v.dt >= LOWER(ar.time_range)
        AND v.dt < UPPER(ar.time_range)
        AND ar.problem_level IN ('do-not-use', 'questionable')
    WHERE
        ar.time_range IS NULL --anti join anomalous ranges
);

ALTER TABLE miovision_api.volumes_daily OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.volumes_daily TO bdit_humans;

COMMENT ON VIEW miovision_api.volumes_daily
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