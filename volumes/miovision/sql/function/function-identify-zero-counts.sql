CREATE OR REPLACE FUNCTION miovision_api.identify_zero_counts(
    start_date date --run on multiple dates using a lateral query.
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

BEGIN

    WITH zero_intersections AS (
        SELECT
            intersection_uid,
            MIN(datetime_bin) AS range_start,
            MAX(datetime_bin) + interval '15 minutes' AS range_end
        FROM miovision_api.volumes_15min_mvt
        WHERE
            datetime_bin >= start_date
            AND datetime_bin < start_date + interval '1 day'
        GROUP BY intersection_uid
        HAVING COALESCE(SUM(volume), 0) = 0
    ),
    
    new_gaps AS (
        SELECT
            v15.intersection_uid,
            v15.classification_uid,
            MIN(v15.datetime_bin) AS range_start,
            MAX(v15.datetime_bin) + interval '15 minutes' AS range_end
        FROM miovision_api.volumes_15min_mvt AS v15
        --anti join intersections with periods of zero volume for all modes
        LEFT JOIN zero_intersections AS zi ON 
            zi.intersection_uid = v15.intersection_uid
            AND v15.datetime_bin >= zi.range_start
            AND v15.datetime_bin < zi.range_end
        WHERE
            zi.intersection_uid IS NULL
            AND v15.datetime_bin >= start_date
            AND v15.datetime_bin < start_date + interval '1 day'
            --this script will only catch zeros for classification_uid 1,2,6,10
            --since those are the ones that are zero padded. Filter for speed.
            AND v15.classification_uid IN (1,2,6,10)
        GROUP BY
            v15.classification_uid,
            v15.intersection_uid
        HAVING SUM(volume) = 0

        UNION

        SELECT
            intersection_uid,
            null::integer AS classification_uid,
            range_start,
            range_end
        FROM zero_intersections
    ),
    
    updated_values AS (
        --update new outages which are contiguous with old outages
        UPDATE miovision_api.anomalous_ranges AS existing
        SET
            range_end = new_.range_end
        FROM new_gaps AS new_
        WHERE
            --deal with null values in intersection, classification
            COALESCE(existing.intersection_uid, 0) = COALESCE(new_.intersection_uid, 0)
            AND COALESCE(existing.classification_uid, 0) = COALESCE(new_.classification_uid, 0)
            --only merging ranges that overlap in (0)->(new) direction
            --otherwise if days run out of order we have to deal with
            --recursive situation of collapsing (0)<-(new)->(2)
            AND existing.range_end = new_.range_start
            AND existing.notes = 'Zero counts, identified by a daily airflow process running function miovision_api.identify_zero_counts'
            AND existing.investigation_level = 'auto_flagged'
        --returns the rows from `new_gaps` which were used to modify existing.
        RETURNING new_.*
    )
    
    INSERT INTO miovision_api.anomalous_ranges(
        intersection_uid, classification_uid, range_start, range_end, notes, investigation_level, problem_level)
    SELECT
        new_gaps.intersection_uid,
        new_gaps.classification_uid,
        new_gaps.range_start,
        new_gaps.range_end,
        'Zero counts, identified by a daily airflow process running function miovision_api.identify_zero_counts' AS notes,
        'auto_flagged' AS investigation_level,
        'do-not-use' AS problem_level
    FROM new_gaps
    --anti join values which were already used for above updates
    LEFT JOIN updated_values ON
        COALESCE(new_gaps.intersection_uid, 0) = COALESCE(updated_values.intersection_uid, 0)
        AND COALESCE(new_gaps.classification_uid, 0) = COALESCE(updated_values.classification_uid, 0)
        AND new_gaps.range_start = updated_values.range_start
    --anti join existing open ended gaps
    LEFT JOIN miovision_api.anomalous_ranges AS existing ON
        COALESCE(existing.intersection_uid, 0) = COALESCE(new_gaps.intersection_uid, 0)
        AND COALESCE(existing.classification_uid, 0) = COALESCE(new_gaps.classification_uid, 0)
        AND existing.range_end IS NULL
    WHERE
        existing.uid IS NULL
        AND updated_values.range_start IS NULL
    ON CONFLICT
    DO NOTHING;
END;

$BODY$;

ALTER FUNCTION miovision_api.identify_zero_counts(timestamp, timestamp, interval)
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.identify_zero_counts(timestamp, timestamp, interval)
TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.identify_zero_counts(timestamp, timestamp, interval)
TO miovision_admins;
