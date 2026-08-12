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

WITH movements_to_pad AS (
    --vehicle & bike entries
    SELECT DISTINCT
        im.intersection_uid,
        im.classification_uid,
        mm.entry_dir AS dir,
        im.leg
    FROM miovision_api.intersection_movements AS im
    JOIN miovision_api.movement_map AS mm USING (movement_uid, leg)
    WHERE
        im.movement_uid <= 4
        AND im.classification_uid IN (1, 2)
    
    UNION

    --vehicle & bike exits
    SELECT DISTINCT
        im.intersection_uid,
        im.classification_uid,
        mm.exit_dir,
        mm.exit_leg
    FROM miovision_api.intersection_movements AS im
    JOIN miovision_api.movement_map AS mm USING (movement_uid, leg)
    WHERE
        im.movement_uid <= 4
        AND im.classification_uid IN (1, 2)
       
    UNION
    
    --pedestrian entries
    SELECT DISTINCT
        im.intersection_uid,
        im.classification_uid,
        mm.entry_dir,
        mm.leg
    FROM miovision_api.intersection_movements AS im
    JOIN miovision_api.movement_map AS mm USING (movement_uid, leg)
    WHERE
        im.movement_uid IN (5, 6)
        AND im.classification_uid = 6
        
    UNION
    
    --bike approach entries
    SELECT DISTINCT
        im.intersection_uid,
        im.classification_uid,
        mm.entry_dir,
        mm.leg
    FROM miovision_api.intersection_movements AS im
    JOIN miovision_api.movement_map AS mm USING (movement_uid, leg)
    WHERE
        im.movement_uid = 7
        AND im.classification_uid = 10
    ORDER BY classification_uid, leg, dir  
),

--check, this should be 28
--SELECT DISTINCT classification_uid, leg, dir FROM movements_to_pad

temp AS (
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
    --zero padding of valid entry/exit movements
    SELECT
        i.intersection_uid,
        gs.datetime_bin,
        m2p.classification_uid,
        m2p.leg,
        m2p.dir,
        0 AS volume
    FROM miovision_api.intersections AS i
    JOIN movements_to_pad AS m2p USING (intersection_uid),
    generate_series(
        greatest(i.date_installed, start_date),
        least(i.date_decommissioned, end_date),
        '15 minutes'::interval
    ) AS gs(datetime_bin)
    
)

INSERT INTO miovision_api.volumes_15min_atr_unfiltered_table (
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
