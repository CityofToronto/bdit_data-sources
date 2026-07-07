-- DROP FUNCTION miovision_api.volumes_15min_mvt_filtered (int[], int[], date, date);

CREATE OR REPLACE FUNCTION miovision_api.volumes_15min_mvt_filtered(
    intersection_uids int[],
    classification_uids int[],
    date_start date,
    date_end date
)
RETURNS TABLE (
intersection_uid integer,
datetime_bin timestamp without time zone,
classification_uid integer,
leg text,
movement_uid int,
volume numeric
) AS $$
BEGIN
    
    RETURN QUERY
    
    WITH v15 AS (
        SELECT
            v15.intersection_uid,
            v15.datetime_bin,
            v15.classification_uid,
            v15.leg,
            v15.movement_uid,
            v15.volume
        FROM miovision_api.volumes_15min_mvt_unfiltered AS v15
        WHERE
            v15.intersection_uid = ANY(volumes_15min_mvt_filtered.intersection_uids)
            AND v15.classification_uid = ANY(volumes_15min_mvt_filtered.classification_uids)
            AND v15.datetime_bin >= volumes_15min_mvt_filtered.date_start
            AND v15.datetime_bin < volumes_15min_mvt_filtered.date_end
    )
    
    SELECT
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        v15.movement_uid,
        v15.volume
    FROM v15
    --anti join unacceptable_gaps
    LEFT JOIN miovision_api.unacceptable_gaps AS un USING (datetime_bin, intersection_uid)
    --anti join anomalous_ranges
    LEFT JOIN miovision_api.anomalous_ranges AS ar
        ON (ar.problem_level = ANY(ARRAY['do-not-use'::text, 'questionable'::text]))
        AND (
            ar.intersection_uid = v15.intersection_uid
            OR ar.intersection_uid IS NULL
        ) AND (
            ar.classification_uid = v15.classification_uid
            --don't use any vehicle modes if lights are anomalous
            OR (ar.classification_uid = 1 AND v15.classification_uid IN (1, 3, 4, 5, 9))
            --issue affects all modes
            OR ar.classification_uid IS NULL
        ) AND (
            ar.leg = v15.leg
            OR ar.leg IS NULL
        )
        AND v15.datetime_bin >= ar.range_start
        AND (
            v15.datetime_bin <= ar.range_end
            OR ar.range_end IS NULL
        )
    WHERE
        ar.uid IS NULL
        AND un.datetime_bin IS NULL;
    END;

$$ LANGUAGE plpgsql;

ALTER FUNCTION miovision_api.volumes_15min_mvt_filtered (int[], int[], date, date) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.volumes_15min_mvt_filtered (int[], int[], date, date) TO bdit_humans;

/*
--7s, vs 2.5 minutes with view
SELECT * FROM miovision_api.volumes_15min_mvt_filtered(
    intersection_uids := '{159,164,162,161,160,158,163,151,152,153,154,155,156,157}'::int[],
    classification_uids := '{1}'::int[],
    date_start := '2026-05-01',
    date_end := CURRENT_DATE
)
*/

-- DROP FUNCTION miovision_api.volumes_15min_mvt_filtered (int[], date, date);

CREATE OR REPLACE FUNCTION miovision_api.volumes_15min_mvt_filtered(
    intersection_uids int[],
    date_start date,
    date_end date
)
RETURNS TABLE (
intersection_uid integer,
datetime_bin timestamp without time zone,
classification_uid integer,
leg text,
movement_uid integer,
volume numeric
) AS $$
BEGIN
    
    RETURN QUERY

    SELECT *
    FROM miovision_api.volumes_15min_mvt_filtered(
        intersection_uids := volumes_15min_mvt_filtered.intersection_uids,
        classification_uids := (SELECT array_agg(classifications.classification_uid) FROM miovision_api.classifications),
        date_start := volumes_15min_mvt_filtered.date_start,
        date_end := volumes_15min_mvt_filtered.date_end
    );
    END;

$$ LANGUAGE plpgsql;

ALTER FUNCTION miovision_api.volumes_15min_mvt_filtered (int[], date, date) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.volumes_15min_mvt_filtered (int[], date, date) TO bdit_humans;

/*
--54s function, vs 11s with view.
--1s for 1 intersection with both methods
SELECT * FROM miovision_api.volumes_15min_mvt_filtered(
    intersection_uids := '{159,164,162,161,160,158,163,151,152,153,154,155,156,157}'::int[],
    date_start := '2026-05-01',
    date_end := CURRENT_DATE
)
*/