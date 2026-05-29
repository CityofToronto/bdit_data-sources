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

INSERT INTO miovision_api.volumes_15min_atr_unfiltered_table (intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
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
GROUP BY v.intersection_uid, datetime_bin_15(v.datetime_bin), v.classification_uid, mmm.exit_leg, mmm.exit_dir;

RAISE NOTICE '% Done aggregating to 15min ATR bins', timeofday();
END;

$BODY$;

ALTER FUNCTION miovision_api.aggregate_15_min_atr(date, date, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_atr(date, date, integer [])
TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.aggregate_15_min_atr(date, date, integer [])
IS '''Aggregates valid movements from `miovision_api.volumes` in to
`miovision_api.volumes_15min_mvt_unfiltered` as 15 minute turning movement counts (TMC) bins and fills
in gaps with 0-volume bins. Also updates foreign key in `miovision_api.volumes`. Takes an
optional intersection array parameter to aggregate only specific intersections. Use
`clear_15_min_mvt()` to remove existing values before summarizing.''';

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_atr(date, date, integer [])
TO miovision_admins;
