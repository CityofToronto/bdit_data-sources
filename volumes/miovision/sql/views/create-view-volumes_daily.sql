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
        v.avg_historical_gap_vol,
        array_agg(ar.notes ORDER BY ar.range_start, ar.uid) FILTER (WHERE ar.uid IS NOT NULL)
        AS anomalous_range_caveats,
        array_agg(ar.uid ORDER BY ar.range_start, ar.uid) FILTER (WHERE ar.uid IS NOT NULL)
        AS anomalous_range_uids
    FROM miovision_api.volumes_daily_unfiltered AS v
    --left join anomalous_ranges to get notes
    --exclude ['do-not-use', 'questionable'] in HAVING
    LEFT JOIN miovision_api.anomalous_ranges AS ar
        ON (
            ar.intersection_uid = v.intersection_uid
            OR ar.intersection_uid IS NULL
        ) AND (
            ar.classification_uid = v.classification_uid
            OR ar.classification_uid IS NULL
        )
        --omitting join to ar.leg implies that a single leg outage is
        --sufficient to exclude entire intersection.
        AND v.dt >= ar.range_start
        AND (
            v.dt < ar.range_end
            OR ar.range_end IS NULL
        )
    GROUP BY
        v.dt,
        v.intersection_uid,
        v.classification_uid,
        v.daily_volume,
        v.isodow,
        v.holiday,
        v.datetime_bins_missing,
        v.unacceptable_gap_minutes,
        v.avg_historical_gap_vol
    HAVING NOT (array_agg(ar.problem_level) && ARRAY['do-not-use', 'questionable'])
    ORDER BY
        v.dt,
        v.intersection_uid,
        v.classification_uid
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
IS 'Consecutive minutes with zero volume deemed unacceptable for use based on a
threshold set using avg historical intersection volume in the same hour.';

COMMENT ON COLUMN miovision_api.volumes_daily.datetime_bins_missing
IS 'Minutes with zero volumes out of a total of possible 1440 minutes per day.';

COMMENT ON COLUMN miovision_api.volumes_daily.avg_historical_gap_vol
IS 'Avg historical volume for that classification and gap duration 
based on averages from a 60 day lookback in that hour.';

COMMENT ON COLUMN miovision_api.volumes_daily.anomalous_range_caveats
IS 'Notes from relelvant anomalous_ranges that are not either
    ''do-not-use'' or ''questionable''.';

COMMENT ON COLUMN miovision_api.volumes_daily.anomalous_range_uids
IS 'The `uid`s of relelvant anomalous_ranges that are not either
    ''do-not-use'' or ''questionable''.';
