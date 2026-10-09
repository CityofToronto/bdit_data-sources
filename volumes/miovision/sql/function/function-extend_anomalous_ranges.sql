CREATE OR REPLACE FUNCTION miovision_api.extend_anomalous_ranges(ds date)
RETURNS void AS $$

WITH ranges_to_extend_backwards AS (
    SELECT
        ar.uid,
        ar.range_start,
        COALESCE(MAX(v.datetime_bin), ar.range_start - '1 day'::interval) AS range_start_new
    FROM miovision_api.anomalous_ranges AS ar
    LEFT JOIN miovision_api.volumes AS v ON
        ar.intersection_uid = v.intersection_uid
        AND (ar.classification_uid = v.classification_uid OR ar.classification_uid IS NULL)
        AND (ar.leg = v.leg OR ar.leg IS NULL)
        AND v.datetime_bin < ar.range_start
        AND v.datetime_bin >= ar.range_start - '1 day'::interval
    WHERE
        --we can extend backwards ranges that started today
        ar.range_start = extend_anomalous_ranges.ds
        AND ar.investigation_level = 'auto_flagged'
        AND ar.notes = 'Zero counts, identified by a daily airflow process running function miovision_api.identify_zero_counts'
    GROUP BY ar.uid
),

ranges_to_extend_forwards AS (
    SELECT
        ar.uid,
        ar.range_end,
        COALESCE(MIN(v.datetime_bin), ar.range_start + '1 day'::interval) AS range_end_new
    FROM miovision_api.anomalous_ranges AS ar
    LEFT JOIN miovision_api.volumes AS v ON
        ar.intersection_uid = v.intersection_uid
        AND (ar.classification_uid = v.classification_uid OR ar.classification_uid IS NULL)
        AND (ar.leg = v.leg OR ar.leg IS NULL)
        AND v.datetime_bin >= ar.range_end
        AND v.datetime_bin < ar.range_end + '1 day'::interval
    WHERE
        --we can only extend forwards ranges that ended today
        ar.range_end = extend_anomalous_ranges.ds
        AND ar.investigation_level = 'auto_flagged'
        AND ar.notes = 'Zero counts, identified by a daily airflow process running function miovision_api.identify_zero_counts'
    GROUP BY ar.uid
),

update1 AS (
    UPDATE miovision_api.anomalous_ranges AS ar
    SET range_start = range_start_new
    FROM ranges_to_extend_backwards AS ar_to_update
    WHERE ar.uid = ar_to_update.uid
)

--update2
UPDATE miovision_api.anomalous_ranges AS ar
SET range_end = range_end_new
FROM ranges_to_extend_forwards AS ar_to_update
WHERE ar.uid = ar_to_update.uid;

$$ LANGUAGE sql;

COMMENT ON FUNCTION miovision_api.extend_anomalous_ranges
IS 'Extends anomalous ranges forward and backwards until the first available data (24hr search window).';

ALTER FUNCTION miovision_api.extend_anomalous_ranges
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.extend_anomalous_ranges TO miovision_api_bot;
