CREATE OR REPLACE FUNCTION miovision_api.aggregate_15_min_mvt(
    start_date date,
    end_date date,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);

BEGIN

WITH temp AS (
    -- Cross product of dates, intersections, legal movement for cars, bikes, and peds to aggregate
    SELECT
        im.intersection_uid,
        dt.datetime_bin,
        im.classification_uid,
        im.leg,
        im.movement_uid,
        0 AS volume
    FROM miovision_api.intersection_movements AS im
    CROSS JOIN generate_series(
        start_date,
        end_date - interval '15 minutes',
        interval '15 minutes'
    ) AS dt(datetime_bin)
    WHERE
        --0 padding for certain modes (padding)
        im.classification_uid IN (1,2,6,10)
        AND im.intersection_uid = ANY(target_intersections)
        
    UNION ALL
    
    --real volumes
    SELECT
        v.intersection_uid,
        datetime_bin_15(v.datetime_bin) AS datetime_bin,
        v.classification_uid,
        v.leg,
        v.movement_uid,
        SUM(volume)
    FROM miovision_api.volumes AS v
    --only aggregate common movements
    JOIN miovision_api.intersection_movements USING (
        intersection_uid, classification_uid, leg, movement_uid
    )
    WHERE
        v.datetime_bin >= start_date
        AND v.datetime_bin < end_date
        AND v.intersection_uid = ANY(target_intersections)
    GROUP BY
        v.intersection_uid,
        datetime_bin_15(v.datetime_bin),
        v.classification_uid,
        v.leg,
        v.movement_uid
),

aggregate_insert AS (
    INSERT INTO miovision_api.volumes_15min_mvt(
        intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume
    )
    SELECT DISTINCT ON (
        v.intersection_uid, v.datetime_bin, v.classification_uid, v.leg, v.movement_uid
    )
        v.intersection_uid,
        v.datetime_bin,
        v.classification_uid,
        v.leg,
        v.movement_uid,
        CASE
            --set unacceptable gaps as nulls
            WHEN un.datetime_bin IS NOT NULL THEN NULL
            --gap fill with zeros (restricted to certain modes in temp CTE)
            ELSE v.volume
        END AS volume
    FROM temp AS v
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    --set unacceptable gaps as null
    LEFT JOIN miovision_api.unacceptable_gaps AS un USING (
        intersection_uid, datetime_bin
    )
    WHERE
        -- Only include dates during which intersection is active 
        -- (excludes entire day it was added/removed)
        v.datetime_bin >= i.date_installed + interval '1 day'
        AND (
            i.date_decommissioned IS NULL
            OR (v.datetime_bin < i.date_decommissioned - interval '1 day')
        )
    ORDER BY 
        v.intersection_uid,
        v.datetime_bin,
        v.classification_uid,
        v.leg,
        v.movement_uid,
        --select real value instead of padding value if available
        v.volume DESC
    RETURNING intersection_uid, volume_15min_mvt_uid, datetime_bin, classification_uid, leg, movement_uid, volume
)

--To update foreign key for 1min bin table
UPDATE miovision_api.volumes AS v
SET volume_15min_mvt_uid = a_i.volume_15min_mvt_uid
FROM aggregate_insert AS a_i
WHERE
    v.datetime_bin >= start_date
    AND v.datetime_bin < end_date
    AND v.volume_15min_mvt_uid IS NULL
    AND a_i.volume > 0
    AND v.intersection_uid = a_i.intersection_uid
    AND v.datetime_bin >= a_i.datetime_bin
    AND v.datetime_bin < a_i.datetime_bin + interval '15 minutes'
    AND v.classification_uid = a_i.classification_uid
    AND v.leg = a_i.leg
    AND v.movement_uid = a_i.movement_uid;

RAISE NOTICE '% Done aggregating to 15min MVT bin', timeofday();
END;

$BODY$;

ALTER FUNCTION miovision_api.aggregate_15_min_mvt(date, date, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_mvt(date, date, integer [])
TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.aggregate_15_min_mvt(date, date, integer []) 
IS '''Aggregates valid movements from `miovision_api.volumes` in to
`miovision_api.volumes_15min_mvt` as 15 minute turning movement counts (TMC) bins and fills
in gaps with 0-volume bins. Also updates foreign key in `miovision_api.volumes`. Takes an
optional intersection array parameter to aggregate only specific intersections. Use
`clear_15_min_mvt()` to remove existing values before summarizing.''';

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_mvt(date, date, integer [])
TO miovision_admins;