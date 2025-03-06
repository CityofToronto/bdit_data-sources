WITH valid_legs AS (
    SELECT
        intersection_uid,
        'E' AS leg
    FROM miovision_api.active_intersections
    WHERE e_leg_restricted IS NULL
    UNION
    SELECT
        intersection_uid,
        'W' AS leg
    FROM miovision_api.active_intersections
    WHERE w_leg_restricted IS NULL
    UNION
    SELECT
        intersection_uid,
        'N' AS leg
    FROM miovision_api.active_intersections
    WHERE n_leg_restricted IS NULL
    UNION
    SELECT
        intersection_uid,
        'S' AS leg
    FROM miovision_api.active_intersections
    WHERE s_leg_restricted IS NULL
),

missing AS (
    SELECT
        vl.intersection_uid,
        vl.leg,
        CASE
            WHEN
                cl.centreline_id IS NULL
                THEN 'Entry missing from `miovision_api.centreline_miovision`.'
            WHEN
                latest.centreline_id IS NULL
                THEN 'Entry is outdated (no longer in `gis_core.centreline_latest`).'
        END AS description
    FROM valid_legs AS vl
    JOIN miovision_api.active_intersections USING (intersection_uid)
    LEFT JOIN miovision_api.centreline_miovision AS cl USING (intersection_uid, leg)
    LEFT JOIN gis_core.centreline_latest AS latest USING (centreline_id)
    WHERE
        cl.intersection_uid IS NULL --not in table (allowing for nulls)
        --entry exists, but is no longer valid
        OR (cl.centreline_id IS NOT NULL AND latest.centreline_id IS NULL)
    ORDER BY intersection_uid
)

SELECT
    NOT(COUNT(*) > 0) AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*)
    || ' Miovision legs which are missing/outdated in `miovision_api.centreline_miovision`.'
    AS summ,
    array_agg(
        'intersection_uid: `' || intersection_uid
        || '`, leg: `' || leg || '` '
        || description
    ) AS gaps
FROM missing
