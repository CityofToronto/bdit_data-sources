CREATE OR REPLACE FUNCTION miovision_api.aggregate_15_min_mvt_single_intersection(
    start_date date,
    end_date date,
    target_intersection int)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

BEGIN

WITH aggregate_insert AS (
    INSERT INTO miovision_api.volumes_15min_mvt(
        intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume
    )
    SELECT
        im.intersection_uid,
        dt.datetime_bin,
        im.classification_uid,
        im.leg,
        im.movement_uid,
        CASE
            --set unacceptable gaps as nulls
            WHEN un.accept = FALSE THEN NULL
            --gap fill with zeros (restricted to certain modes in having clause)
            ELSE (COALESCE(SUM(v.volume), 0))
        END AS volume
    -- Cross product of dates, intersections, legal movement for cars, bikes, and peds to aggregate
    FROM miovision_api.intersection_movements AS im
    CROSS JOIN generate_series(
        start_date,
        end_date - interval '15 minutes',
        interval '15 minutes'
    ) AS dt(datetime_bin)
    JOIN miovision_api.intersections AS mai USING (intersection_uid)
    --add an anti join on destination table to prevent duplicate key errors with zero padding movements.
    LEFT JOIN miovision_api.volumes_15min_mvt AS v15mvt USING (
        intersection_uid, datetime_bin, classification_uid, leg, movement_uid
    )
    --To avoid aggregating unacceptable gaps
    LEFT JOIN miovision_api.unacceptable_gaps AS un ON
        un.intersection_uid = im.intersection_uid
        --remove the complete hour containing any unacceptable gaps
        AND dt.datetime_bin >= date_trunc('hour', un.gap_start)
        AND dt.datetime_bin < date_trunc('hour', un.gap_end) + interval '1 hour'
        --only join unacceptable gaps labeled as do not accept.
        AND un.accept = FALSE
    --To get 1min bins
    LEFT JOIN miovision_api.volumes AS v ON
        --help query choose correct partition
        v.datetime_bin >= start_date
        AND v.datetime_bin < end_date
        AND v.datetime_bin >= dt.datetime_bin
        AND v.datetime_bin < dt.datetime_bin + interval '15 minutes'
        AND v.intersection_uid = im.intersection_uid
        AND v.classification_uid = im.classification_uid
        AND v.leg = im.leg
        AND v.movement_uid = im.movement_uid
    WHERE
        mai.intersection_uid = target_intersection
        -- Only include dates during which intersection is active 
        -- (excludes entire day it was added/removed)
        AND dt.datetime_bin > mai.date_installed + interval '1 day'
        AND (
            mai.date_decommissioned IS NULL
            OR (dt.datetime_bin < mai.date_decommissioned - interval '1 day')
        )
        --exclude movements already aggregated
        AND v.volume_15min_mvt_uid IS NULL
        --exclude values already in destination table
        AND v15mvt.intersection_uid IS NULL
    GROUP BY
        im.intersection_uid,
        dt.datetime_bin,
        im.classification_uid,
        im.leg,
        im.movement_uid,
        un.accept
    HAVING
        --retain 0s for certain modes (padding)
        im.classification_uid IN (1,2,6,10)
        OR SUM(v.volume) > 0  
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

ALTER FUNCTION miovision_api.aggregate_15_min_mvt_single_intersection(date, date, int)
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_mvt_single_intersection(date, date, int) TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_mvt_single_intersection(date, date, int) TO miovision_admins;