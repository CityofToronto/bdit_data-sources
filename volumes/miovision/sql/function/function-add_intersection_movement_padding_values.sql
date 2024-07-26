CREATE OR REPLACE FUNCTION miovision_api.fn_add_intersection_movement_padding_values()
RETURNS trigger
LANGUAGE 'plpgsql'

COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE n_inserted numeric;

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
    FROM new_rows AS im
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    CROSS JOIN generate_series(
        i.date_installed,
        LEAST(
            i.date_decommissioned,
            (CURRENT_TIMESTAMP AT TIME ZONE 'Canada/Eastern')::date
        ) - interval '15 minutes', 
        interval '15 minutes'
    ) AS dt(datetime_bin)
    WHERE
        --0 padding for certain modes (padding)
        im.classification_uid IN (1,2,6,10)
        
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
    JOIN new_rows USING (
        intersection_uid, classification_uid, leg, movement_uid
    )
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

updated AS (
    --To update foreign key for 1min bin table
    UPDATE miovision_api.volumes AS v
    SET volume_15min_mvt_uid = a_i.volume_15min_mvt_uid
    FROM aggregate_insert AS a_i
    WHERE
        v.volume_15min_mvt_uid IS NULL
        AND a_i.volume > 0
        AND v.intersection_uid = a_i.intersection_uid
        AND v.datetime_bin >= a_i.datetime_bin
        AND v.datetime_bin < a_i.datetime_bin + interval '15 minutes'
        AND v.classification_uid = a_i.classification_uid
        AND v.leg = a_i.leg
        AND v.movement_uid = a_i.movement_uid
)

SELECT COUNT(*) INTO n_inserted
FROM aggregate_insert;

RAISE NOTICE '% Done adding to 15min MVT bin based on intersection_movement new rows. % rows added.', timeofday(), n_inserted;
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