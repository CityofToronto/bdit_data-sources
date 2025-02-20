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

    --intersections with zero volume
    WITH all_new_gaps AS (
        SELECT
            v15.intersection_uid,
            v15.classification_uid,
            v15.leg,
            MIN(v15.datetime_bin) AS range_start,
            MAX(v15.datetime_bin) + interval '15 minutes' AS range_end
        FROM miovision_api.volumes_15min_mvt_unfiltered AS v15
        WHERE
            v15.datetime_bin >= start_date
            AND v15.datetime_bin < start_date + interval '1 day'
            --this script will only catch zeros for classification_uid 1,2,6,10
            --since those are the ones that are zero padded in volumes_15min_mvt_unfiltered. Filter for additional speed.
            AND v15.classification_uid IN (1,2,6,10)
            AND v15.intersection_uid = ANY(target_intersections)
        GROUP BY
            GROUPING SETS (
                (v15.intersection_uid), --implies null class, leg
                (v15.intersection_uid, v15.classification_uid), --implies null leg
                (v15.intersection_uid, v15.classification_uid, v15.leg)
            )
        HAVING COALESCE(SUM(volume), 0) = 0
    ),

    new_gaps AS (
        --get intersection outages
        SELECT
            zero_int.intersection_uid,
            zero_int.classification_uid,
            zero_int.leg,
            zero_int.range_start,
            zero_int.range_end
        FROM all_new_gaps AS zero_int
        WHERE classification_uid IS NULL --identifies int outage

        UNION

        --get intersection-classification outages
        SELECT
            zero_int_class.intersection_uid,
            zero_int_class.classification_uid,
            zero_int_class.leg,
            zero_int_class.range_start,
            zero_int_class.range_end
        FROM all_new_gaps AS zero_int_class
        --anti join intersection outages
        LEFT JOIN all_new_gaps AS zero_int
        ON
            zero_int.classification_uid IS NULL
            AND zero_int.intersection_uid = zero_int_class.intersection_uid
        WHERE
            zero_int_class.leg IS NULL --identifies int-class outage
            AND zero_int.intersection_uid IS NULL --anti join

        UNION

        --get intersection-classification-leg outages
        SELECT
            zero_int_class_leg.intersection_uid,
            zero_int_class_leg.classification_uid,
            zero_int_class_leg.leg,
            zero_int_class_leg.range_start,
            zero_int_class_leg.range_end
        FROM all_new_gaps AS zero_int_class_leg
        --anti join intersection outages
        LEFT JOIN all_new_gaps AS zero_int
        ON
            zero_int.classification_uid IS NULL
            AND zero_int.classification_uid = zero_int_class_leg.classification_uid
        --anti join intersection-classification-outages
        LEFT JOIN all_new_gaps AS zero_int_class
        ON
            zero_int_class.leg IS NULL
            AND zero_int_class.intersection_uid = zero_int_class_leg.intersection_uid
            AND zero_int_class.classification_uid = zero_int_class_leg.classification_uid
        WHERE
            zero_int_class_leg.leg IS NOT NULL  --identifies int-class-leg outage
            AND zero_int.intersection_uid IS NULL --anti join
            AND zero_int_class.intersection_uid IS NULL  --anti join
    ),

    updated_values AS (
        --update new outages which are contiguous with old outages
        UPDATE miovision_api.anomalous_ranges AS existing
        SET
            range_end = new_.range_end
        FROM new_gaps AS new_
        WHERE
            --deal with null values in intersection, classification, leg
            COALESCE(existing.intersection_uid, 0) = COALESCE(new_.intersection_uid, 0)
            AND COALESCE(existing.classification_uid, 0) = COALESCE(new_.classification_uid, 0)
            AND COALESCE(existing.leg, '0') = COALESCE(new_.leg, '0')
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
        intersection_uid, classification_uid, leg, range_start, range_end, notes, investigation_level, problem_level)
    SELECT
        new_gaps.intersection_uid,
        new_gaps.classification_uid,
        new_gaps.leg,
        new_gaps.range_start,
        new_gaps.range_end,
        'Zero counts, identified by a daily airflow process running function miovision_api.identify_zero_counts' AS notes,
        'auto_flagged' AS investigation_level,
        'do-not-use' AS problem_level
    FROM new_gaps
    --anti join values which were already used for above updates
    LEFT JOIN updated_values ON
        --there are no null intersection values in new_gaps.intersection_uid so don't need a null case.
        new_gaps.intersection_uid = updated_values.intersection_uid
        AND COALESCE(new_gaps.classification_uid, 0) = COALESCE(updated_values.classification_uid, 0)
        AND COALESCE(new_gaps.leg, '0') = COALESCE(updated_values.leg, '0')
        AND new_gaps.range_start = updated_values.range_start
    --anti join with existing gaps to avoid adding new overlapping gaps
    LEFT JOIN miovision_api.anomalous_ranges AS existing ON
        --there are no null intersections in new_gaps to deal with
        existing.intersection_uid = new_gaps.intersection_uid
        --exclude if same classification_uid, or existing is null (all)
        AND (
            existing.classification_uid = new_gaps.classification_uid
            OR existing.classification_uid IS NULL
        ) AND (
            existing.leg = new_gaps.leg
            OR existing.leg IS NULL
        ) AND (
            --exclude any overlapping records
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
        existing.uid IS NULL --an overlapping gap doesn't already exist 
        AND updated_values.range_start IS NULL --row not already used for update
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

COMMENT ON FUNCTION miovision_api.identify_zero_counts(date, integer [])
IS 'Identifies intersection / classification (only 1,2,6,10) / leg
combos with zero volumes for the start_date called. Inserts or updates anomaly
into anomalous_range table unless an existing, overlapping, manual entery exists.';
