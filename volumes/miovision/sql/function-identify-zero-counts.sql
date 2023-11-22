CREATE OR REPLACE FUNCTION miovision_api.identify_zero_counts(
    start_date timestamp,
    end_date timestamp)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

BEGIN

WITH zero_intersections AS (
        SELECT
            intersection_uid,
            null::integer AS classification_uid,
            tsrange(MIN(datetime_bin), MAX(datetime_bin) + interval '15 minutes', '[)') AS time_range,
            'zero counts' AS notes,
            'auto_flagged' AS investigation_level,
            'do-not-use' AS problem_level
        FROM miovision_api.volumes_15min_mvt
        WHERE
            classification_uid IN (1,2,6,10) 
            AND datetime_bin >= start_date
            AND datetime_bin < end_date
        GROUP BY intersection_uid
        HAVING
            SUM(volume) = 0
            --duration filter
            AND (MAX(datetime_bin) - MIN(datetime_bin) + interval '15 minutes') >= '1 hour'::interval
    )

    INSERT INTO miovision_api.anomalous_ranges(
        intersection_uid, classification_uid, time_range, notes, investigation_level, problem_level
    )
    SELECT
        v15.intersection_uid,
        v15.classification_uid,
        tsrange(MIN(v15.datetime_bin), MAX(v15.datetime_bin) + interval '15 minutes', '[)') AS time_range,
        'zero counts' AS notes,
        'auto_flagged' AS investigation_level,
        'do-not-use' AS problem_level
    FROM miovision_api.volumes_15min_mvt AS v15
    --anti join intersections with periods of zero volume for all modes
    LEFT JOIN zero_intersections AS zi ON 
        zi.intersection_uid = v15.intersection_uid
        AND v15.datetime_bin >= LOWER(zi.time_range)
        AND v15.datetime_bin < UPPER(zi.time_range)
    WHERE
        zi.intersection_uid IS NULL
        AND datetime_bin >= start_date
        AND datetime_bin < end_date
        --this script will only catch zeros for classification_uid 1,2,6,10
        --since those are the ones that are zero padded. Filter for speed.
        AND v15.classification_uid IN (1,2,6,10)
    GROUP BY
        v15.classification_uid,
        v15.intersection_uid
    HAVING
        SUM(volume) = 0
        --duration filter
        AND (MAX(datetime_bin) - MIN(datetime_bin) + interval '15 minutes') >= '1 hour'::interval

    UNION

    SELECT
        intersection_uid,
        classification_uid,
        time_range,
        notes,
        investigation_level,
        problem_level
    FROM zero_intersections
    ON CONFLICT (intersection_uid, classification_uid, time_range)
    --possibility of manual entries to this table, do not overwrite with automated ones.
    DO NOTHING;

END;

$BODY$;

ALTER FUNCTION miovision_api.identify_zero_counts(timestamp, timestamp) OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.identify_zero_counts(timestamp, timestamp) TO miovision_api_bot;
GRANT EXECUTE ON FUNCTION miovision_api.identify_zero_counts(timestamp, timestamp) TO miovision_admins;