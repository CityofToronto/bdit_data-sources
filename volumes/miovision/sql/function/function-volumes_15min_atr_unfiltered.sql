CREATE OR REPLACE FUNCTION miovision_api.aggregate_15_min_atr(
    start_date date,
    end_date date,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
LANGUAGE plpgsql

COST 100
VOLATILE
AS $BODY$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);

BEGIN

WITH temp AS (
    --real entries
    SELECT
        v.intersection_uid,
        datetime_bin_15(v.datetime_bin) AS datetime_bin,
        v.classification_uid,
        v.leg,
        mmm.entry_dir AS dir,
        sum(v.volume)::integer AS volume
    FROM miovision_api.volumes AS v
    JOIN miovision_api.movement_map AS mmm USING (movement_uid, leg)
    WHERE
        v.intersection_uid = ANY(target_intersections)
        AND v.datetime_bin > start_date
        AND v.datetime_bin < end_date
    GROUP BY v.intersection_uid, datetime_bin_15(v.datetime_bin), v.classification_uid, v.leg, mmm.entry_dir
    UNION ALL
    --real exits
    SELECT
        v.intersection_uid,
        datetime_bin_15(v.datetime_bin) AS datetime_bin,
        v.classification_uid,
        mmm.exit_leg AS leg,
        mmm.exit_dir AS dir,
        sum(v.volume)::integer AS volume
    FROM miovision_api.volumes AS v
    JOIN miovision_api.movement_map AS mmm USING (movement_uid, leg)
    WHERE
        v.intersection_uid = ANY(target_intersections)
        AND mmm.exit_leg IS NOT NULL
        AND v.datetime_bin > start_date
        AND v.datetime_bin < end_date
    GROUP BY v.intersection_uid, datetime_bin_15(v.datetime_bin), v.classification_uid, mmm.exit_leg, mmm.exit_dir
    
    UNION ALL
    
    --zero padding of valid entry movements
    SELECT
        i.intersection_uid,
        gs.datetime_bin,
        m2p.classification_uid,
        m2p.leg,
        m2p.dir,
        0 AS volume
    FROM miovision_api.intersections AS i
    JOIN miovision_api.atr_movements_to_pad AS m2p USING (intersection_uid)
    JOIN miovision_api.movement_map AS mmm
        ON m2p.leg = mmm.leg
        AND m2p.dir = mmm.entry_dir,
    generate_series(
        greatest(i.date_installed, start_date, '2019-01-01'::date), --this schema only stores data >= 2019
        least(i.date_decommissioned, end_date) - interval '15 minutes',
        '15 minutes'::interval
    ) AS gs(datetime_bin)
    WHERE
        i.intersection_uid = ANY(target_intersections)
    
    UNION ALL

    --zero padding of valid exit movements
    SELECT
        i.intersection_uid,
        gs.datetime_bin,
        m2p.classification_uid,
        mmm.exit_leg,
        mmm.exit_dir,
        0 AS volume
    FROM miovision_api.intersections AS i
    JOIN miovision_api.atr_movements_to_pad AS m2p USING (intersection_uid)
    JOIN miovision_api.movement_map AS mmm
        ON m2p.leg = mmm.leg
        AND m2p.dir = mmm.entry_dir,
    generate_series(
        greatest(i.date_installed, start_date, '2019-01-01'::date), --this schema only stores data >= 2019
        least(i.date_decommissioned, end_date) - interval '15 minutes',
        '15 minutes'::interval
    ) AS gs(datetime_bin)
    WHERE
        i.intersection_uid = ANY(target_intersections)
        AND exit_leg IS NOT NULL
)

INSERT INTO miovision_api.volumes_15min_atr_unfiltered (
    intersection_uid, datetime_bin, classification_uid, leg, dir, volume
)

SELECT DISTINCT ON (
    intersection_uid, datetime_bin, classification_uid, leg, dir
)
    v.intersection_uid,
    v.datetime_bin,
    v.classification_uid,
    v.leg,
    v.dir,
    v.volume
FROM temp AS v
JOIN miovision_api.intersections AS i USING (intersection_uid)
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
    v.dir,
    --select real value instead of padding value if available
    v.volume DESC;

RAISE NOTICE '% Done aggregating to 15min MVT bin', timeofday();
END;

$BODY$;

ALTER FUNCTION miovision_api.aggregate_15_min_atr(date, date, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_atr(date, date, integer [])
TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.aggregate_15_min_atr(date, date, integer [])
IS '''Aggregates valid movements from `miovision_api.volumes` in to
`miovision_api.volumes_15min_atr_unfiltered` as 15 minute ATR bins and fills
in gaps with 0-volume bins for valid movements. 
Takes an optional intersection array parameter to aggregate only specific intersections. Use
`clear_15_min_atr()` to remove existing values before summarizing.''';

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_atr(date, date, integer [])
TO miovision_admins;
