CREATE OR REPLACE FUNCTION miovision_api.volumes_15min_atr_filtered(
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
dir text,
volume integer
) AS $$
BEGIN
    
    RETURN QUERY
    WITH v15 AS (
        SELECT
            v15.intersection_uid,
            v15.datetime_bin,
            v15.classification_uid,
            v15.leg,
            v15.dir,
            v15.volume
        FROM miovision_api.volumes_15min_atr_unfiltered_table AS v15
        WHERE
            v15.intersection_uid = ANY(volumes_15min_atr_filtered.intersection_uids)
            AND v15.classification_uid = ANY(volumes_15min_atr_filtered.classification_uids)
            AND v15.datetime_bin >= volumes_15min_atr_filtered.date_start
            AND v15.datetime_bin < volumes_15min_atr_filtered.date_end
    )
    
    SELECT
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        v15.dir,
        v15.volume
    FROM v15
        LEFT JOIN miovision_api.unacceptable_gaps USING (datetime_bin, intersection_uid)
    WHERE
        unacceptable_gaps.datetime_bin IS NULL
        AND NOT EXISTS ( --anti join anomalous_ranges
            SELECT 1
            FROM miovision_api.anomalous_ranges AS ar
            WHERE
                ar.problem_level IN ('do-not-use', 'questionable')
                AND (
                    ar.intersection_uid = v15.intersection_uid
                    OR ar.intersection_uid IS NULL
                ) AND (
                    ar.classification_uid = v15.classification_uid
                    --don't use any vehicle modes if lights are anomalous
                    OR (ar.classification_uid = 1 AND v15.classification_uid IN (1, 3, 4, 5, 9))
                    --issue affects all modes
                    OR ar.classification_uid IS NULL
                )
                -- leg is ignored here
                -- any anomalousness on a leg removes all ATR counts
                AND v15.datetime_bin >= ar.range_start
                AND (
                    v15.datetime_bin <= ar.range_end
                    OR ar.range_end IS NULL
                )
        );
    END;

$$ LANGUAGE plpgsql;

ALTER FUNCTION miovision_api.volumes_15min_atr_filtered (int[], int[], date, date) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.volumes_15min_atr_filtered (int[], int[], date, date) TO bdit_humans;

/*
--5s
SELECT * FROM miovision_api.volumes_15min_atr_filtered(
    intersection_uids := '{159,164,162,161,160,158,163,151,152,153,154,155,156,157}'::int[],
    classification_uids := '{1}'::int[],
    date_start := '2026-05-01',
    date_end := CURRENT_DATE
)
*/

CREATE OR REPLACE FUNCTION miovision_api.volumes_15min_atr_filtered(
    intersection_uids int[],
    date_start date,
    date_end date
)
RETURNS TABLE (
intersection_uid integer,
datetime_bin timestamp without time zone,
classification_uid integer,
leg text,
dir text,
volume integer
) AS $$
BEGIN
    
    RETURN QUERY
    SELECT
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        v15.dir,
        v15.volume
    FROM miovision_api.volumes_15min_atr_filtered(
        intersection_uids := volumes_15min_atr_filtered.intersection_uids,
        classification_uids := (SELECT array_agg(classifications.classification_uid) FROM miovision_api.classifications),
        date_start := volumes_15min_atr_filtered.date_start,
        date_end := volumes_15min_atr_filtered.date_end
    ) AS v15;
    END;

$$ LANGUAGE plpgsql;

ALTER FUNCTION miovision_api.volumes_15min_atr_filtered (int[], date, date) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.volumes_15min_atr_filtered (int[], date, date) TO bdit_humans;

/*
--35s, vs 2.5 minutes with view
SELECT * FROM miovision_api.volumes_15min_atr_filtered(
    intersection_uids := '{159,164,162,161,160,158,163,151,152,153,154,155,156,157}'::int[],
    date_start := '2026-05-01',
    date_end := CURRENT_DATE
)
*/