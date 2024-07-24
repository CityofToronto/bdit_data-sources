CREATE OR REPLACE FUNCTION miovision_api.fn_add_intersection_movement_padding_values()
RETURNS trigger
LANGUAGE 'plpgsql'
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

BEGIN

    INSERT INTO miovision_api.volumes_15min_mvt_unfiltered (
        intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume
    )
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
    ON CONFLICT (intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
    --no need to insert a zero if there is already volume.
    DO NOTHING
    RETURN NULL;
END;
$BODY$;

COMMENT ON FUNCTION miovision_api.fn_add_intersection_movement_padding_values() IS
'This function is called using a trigger after each statement on insert into
miovision_api.intersection_movements. It uses newly inserted rows to update the zero padding
values in miovision_api.volumes_15min_mvt_unfiltered';

ALTER FUNCTION miovision_api.fn_add_intersection_movement_padding_values OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.fn_add_intersection_movement_padding_values() TO miovision_api_bot;