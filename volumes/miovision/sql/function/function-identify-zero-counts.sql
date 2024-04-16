CREATE OR REPLACE FUNCTION miovision_api.identify_zero_counts(
    start_date date, --run on multiple dates using a lateral query.
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);
    n_inserted integer;
    n_updated integer;

BEGIN

    --identify intersections with zero volume regardless of mode
    WITH zero_intersections AS (
        SELECT
            intersection_uid,
            MIN(datetime_bin) AS range_start,
            MAX(datetime_bin) + interval '15 minutes' AS range_end
        FROM miovision_api.volumes_15min_mvt
        WHERE
            datetime_bin >= start_date
            AND datetime_bin < start_date + interval '1 day'
            AND intersection_uid = ANY(target_intersections)
        GROUP BY intersection_uid
        HAVING COALESCE(SUM(volume), 0) = 0
    ),
    
    new_gaps AS (
        --identify intersections with zero volume for specific modes
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
            --since those are the ones that are zero padded in volumes_15min_mvt. Filter for additional speed.
            AND v15.classification_uid IN (1,2,6,10)
            AND v15.intersection_uid = ANY(target_intersections)
        GROUP BY
            v15.classification_uid,
            v15.intersection_uid
        HAVING COALESCE(SUM(volume), 0) = 0

        --union with intersections with zero volume regardless of mode
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
            --Only join with other automated zero count anomalous_ranges identified by this process 
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
    --anti join existing open ended/overlapping gaps
    LEFT JOIN miovision_api.anomalous_ranges AS existing ON
        COALESCE(existing.intersection_uid, 0) = COALESCE(new_gaps.intersection_uid, 0)
        AND COALESCE(existing.classification_uid, 0) = COALESCE(new_gaps.classification_uid, 0)
        AND (
            (
                existing.range_start <= new_gaps.range_start
                AND existing.range_end >= new_gaps.range_end
            ) OR (
                existing.range_end IS NULL
                AND existing.range_start <= new_gaps.range_start
            )
        )
    WHERE
        --anti joins
        existing.uid IS NULL
        AND updated_values.range_start IS NULL
    ON CONFLICT
    DO NOTHING;

    --report on new anomalous_ranges:
    SELECT COUNT(*) INTO n_inserted
    FROM miovision_api.anomalous_ranges
    WHERE
        investigation_level = 'auto_flagged'
        AND range_start = start_date
        AND range_end = start_date + interval '1 day';

    RAISE INFO '% records inserted into anomalous_ranges for %.', n_inserted, start_date;
    
    --report on updated anomalous_ranges:
    SELECT COUNT(*) INTO n_updated
    FROM miovision_api.anomalous_ranges
    WHERE
        investigation_level = 'auto_flagged'
        AND range_start <> start_date
        AND range_end = start_date + interval '1 day';
    
    RAISE INFO '% records in anomalous_ranges extended to include %.', n_updated, start_date;

END;

$BODY$;

ALTER FUNCTION miovision_api.identify_zero_counts(date, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.identify_zero_counts(date, integer [])
TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.identify_zero_counts(date, integer [])
TO miovision_admins;

COMMENT ON FUNCTION miovision_api.identify_zero_counts(date)
IS 'Identifies intersection / classification (only classification_uid 1,2,6,10)
combos with zero volumes for the start_date called. Inserts or updates anomaly
into anomalous_range table unless an existing, overlapping, manual entery exists.';