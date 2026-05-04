CREATE OR REPLACE FUNCTION miovision_api.fn_add_intersection_movement_padding_values()
RETURNS trigger
LANGUAGE plpgsql

COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE n_inserted numeric;

BEGIN

WITH temp AS (
    -- Cross product of dates, intersections, legal movement for cars, bikes, and peds to aggregate
    SELECT
        NEW.intersection_uid,
        dt.datetime_bin,
        NEW.classification_uid,
        NEW.leg,
        NEW.movement_uid,
        0 AS volume
    FROM miovision_api.intersections AS i
    CROSS JOIN generate_series(
        GREATEST(i.date_installed, '2019-01-01'::date), --this schema only stores data >= 2019
        LEAST(
            i.date_decommissioned,
            (CURRENT_TIMESTAMP AT TIME ZONE 'Canada/Eastern')::date
        ) - interval '15 minutes', 
        interval '15 minutes'
    ) AS dt(datetime_bin)
    WHERE
        --0 padding for certain modes (padding)
        NEW.classification_uid IN (1,2,6,10)
        AND i.intersection_uid = NEW.intersection_uid
        
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
    WHERE
        v.intersection_uid = NEW.intersection_uid
        AND v.classification_uid = NEW.classification_uid
        AND v.leg = NEW.leg
        AND v.movement_uid = NEW.movement_uid
    GROUP BY
        v.intersection_uid,
        datetime_bin_15(v.datetime_bin),
        v.classification_uid,
        v.leg,
        v.movement_uid
),

aggregate_insert AS (
    INSERT INTO miovision_api.volumes_15min_mvt_unfiltered(
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
        v.volume
    FROM temp AS v
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    --set unacceptable gaps as null
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
    
),

date_bounds AS (
    SELECT
        MIN(datetime_bin) AS min_bin,
        MAX(datetime_bin) AS max_bin
    FROM aggregate_insert
),

updated AS (
    --To update foreign key for 1min bin table
    UPDATE miovision_api.volumes AS v
    SET volume_15min_mvt_uid = a_i.volume_15min_mvt_uid
    FROM aggregate_insert AS a_i,
    date_bounds AS db
    WHERE
        v.volume_15min_mvt_uid IS NULL
        --the min/max date clause really helps to trim the search space
        AND v.datetime_bin >= db.min_bin
        AND v.datetime_bin < db.max_bin + interval '15 minutes'
        AND a_i.volume > 0
        AND v.datetime_bin >= a_i.datetime_bin
        AND v.datetime_bin < a_i.datetime_bin + interval '15 minutes'
        AND v.intersection_uid = NEW.intersection_uid
        AND v.classification_uid = NEW.classification_uid
        AND v.leg = NEW.leg
        AND v.movement_uid = NEW.movement_uid
)

SELECT COUNT(*) INTO n_inserted
FROM aggregate_insert;

RAISE NOTICE '% Done adding to 15min MVT bin based on intersection_movement for intersection_uid=% classification_uid=% leg=% movement_uid=% rows. % rows added.',
timeofday(),
NEW.intersection_uid,
NEW.classification_uid,
NEW.leg,
NEW.movement_uid,
n_inserted;

RETURN NULL; 
END;

$BODY$;

ALTER FUNCTION miovision_api.fn_add_intersection_movement_padding_values()
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.fn_add_intersection_movement_padding_values()
TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.fn_add_intersection_movement_padding_values()
TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.fn_add_intersection_movement_padding_values() IS
'This function is called using a trigger after each statement on insert into
miovision_api.intersection_movements. It uses newly inserted rows to update the zero padding
values in miovision_api.volumes_15min_mvt_unfiltered';