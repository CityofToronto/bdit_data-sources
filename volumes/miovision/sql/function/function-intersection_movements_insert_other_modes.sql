-- FUNCTION: miovision_api.intersection_movements_insert_other_modes()

-- DROP FUNCTION IF EXISTS miovision_api.intersection_movements_insert_other_modes();

CREATE OR REPLACE FUNCTION miovision_api.intersection_movements_insert_other_modes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$

BEGIN

    IF NEW.classification_uid = 1 THEN
        INSERT INTO miovision_api.intersection_movements (
            intersection_uid, classification_uid, leg, movement_uid
        )
        SELECT
            a.intersection_uid,
            other_modes.classification_uid,
            a.leg,
            a.movement_uid
        FROM (VALUES (NEW.intersection_uid, NEW.leg, NEW.movement_uid)) AS a (intersection_uid, leg, movement_uid)
        CROSS JOIN (VALUES (3), (4), (5), (8), (9)) AS other_modes (classification_uid)
        LEFT JOIN miovision_api.intersection_movements
            USING (intersection_uid, classification_uid, leg, movement_uid)
        LEFT JOIN miovision_api.intersection_movements_denylist
            USING (intersection_uid, classification_uid, leg, movement_uid)
        WHERE
            --anti join existing movements
            intersection_movements.intersection_uid IS NULL
            --anti join denylist
            AND intersection_movements_denylist.intersection_uid IS NULL;
    END IF;
    RETURN NULL;

END;
$BODY$;

ALTER FUNCTION miovision_api.intersection_movements_insert_other_modes()
OWNER TO miovision_admins;

REVOKE EXECUTE ON FUNCTION miovision_api.intersection_movements_insert_other_modes() FROM PUBLIC;

GRANT EXECUTE ON FUNCTION miovision_api.intersection_movements_insert_other_modes() TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.intersection_movements_insert_other_modes() TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.intersection_movements_insert_other_modes()
IS 'Runs after insert into miovision_api.intersection_movements to insert non-light auto modes.';
