CREATE OR REPLACE FUNCTION miovision_api.fn_add_intersection_movement_atr_padding_values()
RETURNS trigger
LANGUAGE plpgsql

COST 100
VOLATILE
SECURITY DEFINER
PARALLEL UNSAFE
AS $BODY$

DECLARE n_inserted numeric;

BEGIN

WITH temp AS (
    -- Cross product of dates, intersections, legal movement for cars, bikes, and peds to aggregate
    SELECT
        v.intersection_uid,
        datetime_bin_15(v.datetime_bin) AS datetime_bin,
        v.classification_uid,
        v.leg,
        mmm.entry_dir AS dir,
        sum(v.volume)::integer AS volume
    FROM miovision_api.volumes_2026 AS v
    JOIN miovision_api.movement_map AS mmm USING (movement_uid, leg)
    JOIN miovision_api.atr_movements_to_pad USING (leg, classification_uid, intersection_uid)
    WHERE
        v.intersection_uid = NEW.intersection_uid
        AND v.classification_uid = NEW.classification_uid
        AND v.leg = NEW.leg
    GROUP BY
        v.intersection_uid,
        datetime_bin_15 (v.datetime_bin),
        v.classification_uid,
        v.leg,
        mmm.entry_dir

    UNION ALL

    --real exits
    SELECT
        v.intersection_uid,
        datetime_bin_15(v.datetime_bin) AS datetime_bin,
        v.classification_uid,
        mmm.exit_leg AS leg,
        mmm.exit_dir AS dir,
        sum(v.volume)::integer AS volume
    --make sure to only get classifications we want to pad.
    FROM miovision_api.atr_movements_to_pad AS m2p
    --find where this particular movement exits
    JOIN miovision_api.movement_map AS mmm
        ON m2p.leg = mmm.leg
        AND m2p.dir = mmm.entry_dir
    --then find all the movements that exit there
    JOIN miovision_api.movement_map AS mmm_exit USING (exit_leg, exit_dir)     
    JOIN miovision_api.volumes AS v ON
        v.movement_uid = mmm_exit.movement_uid
        AND v.leg = mmm_exit.leg
        AND v.classification_uid = m2p.classification_uid
        AND v.intersection_uid = m2p.intersection_uid
    WHERE
        m2p.intersection_uid = NEW.intersection_uid
        AND m2p.classification_uid = NEW.classification_uid
        AND m2p.leg = NEW.leg
        AND mmm.movement_uid = NEW.movement_uid
    GROUP BY
        v.intersection_uid,
        datetime_bin_15 (v.datetime_bin),
        v.classification_uid,
        mmm.exit_leg,
        mmm.exit_dir
        
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
        greatest(i.date_installed, '2019-01-01'::date), --this schema only stores data >= 2019
        least(i.date_decommissioned, current_date)::timestamp - interval '15 minutes',
        '15 minutes'::interval
    ) AS gs(datetime_bin)
    WHERE
        m2p.intersection_uid = NEW.intersection_uid
        AND m2p.classification_uid = NEW.classification_uid
        AND m2p.leg = NEW.leg
        AND mmm.movement_uid = NEW.movement_uid
        
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
    --find where this particular movement exits
    JOIN miovision_api.movement_map AS mmm
        ON m2p.leg = mmm.leg
        AND m2p.dir = mmm.entry_dir,
    generate_series(
        greatest(i.date_installed, '2019-01-01'::date), --this schema only stores data >= 2019
        least(i.date_decommissioned, current_date)::timestamp - interval '15 minutes',
        '15 minutes'::interval
    ) AS gs(datetime_bin)
    WHERE
        m2p.intersection_uid = NEW.intersection_uid
        AND m2p.classification_uid = NEW.classification_uid
        AND m2p.leg = NEW.leg
        AND mmm.movement_uid = NEW.movement_uid
),

inserted AS (
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
        v.volume DESC
    ON CONFLICT ON CONSTRAINT volumes_15min_atr_unfiltered_int_dt_bin_class_leg_mvmt_uid_pkey
    DO UPDATE SET volume = EXCLUDED.volume
    RETURNING *
)

SELECT COUNT(*) INTO n_inserted
FROM inserted;

RAISE NOTICE '% Done adding to 15min ATR bin based on intersection_movement for intersection_uid=% classification_uid=% leg=% movement_uid=% rows. % rows added.',
timeofday(),
NEW.intersection_uid,
NEW.classification_uid,
NEW.leg,
NEW.movement_uid,
n_inserted;

RETURN NULL;
END;

$BODY$;

ALTER FUNCTION miovision_api.fn_add_intersection_movement_atr_padding_values()
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.fn_add_intersection_movement_atr_padding_values()
TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.fn_add_intersection_movement_atr_padding_values()
TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.fn_add_intersection_movement_atr_padding_values() IS
'This function is called using a trigger after each statement on insert into
miovision_api.intersection_movements. It uses newly inserted rows to update the zero padding
values in miovision_api.volumes_15min_mvt_unfiltered';