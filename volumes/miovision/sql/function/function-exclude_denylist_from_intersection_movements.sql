CREATE OR REPLACE FUNCTION miovision_api.exclude_denylist_from_intersection_movements()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $BODY$

DECLARE row_in_im numeric = (
    SELECT COUNT(*)
    FROM miovision_api.intersection_movements_denylist AS im
    WHERE
        im.intersection_uid = new.intersection_uid
        AND im.leg = new.leg
        AND im.movement_uid = new.movement_uid
        AND im.classification_uid = new.classification_uid
);

BEGIN
    IF (row_in_im) THEN
        RAISE NOTICE 'Row % is not being inserted. Already in intersection_movements_denylist table.', new;
        --Return null to stop insert. 
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;
END;
$BODY$;

ALTER FUNCTION miovision_api.exclude_denylist_from_intersection_movements OWNER TO miovision_admins;

GRANT EXECUTE ON miovision_api.exclude_denylist_from_intersection_movements TO miovision_api_bot;

COMMENT ON miovision_api.exclude_denylist_from_intersection_movements
IS 'Runs before insert into miovision_api.intersection_movements to prevent inserts that are on
the denylist (miovision_api.intersection_movements_denylist).';

/*Test:
INSERT INTO miovision_api.intersection_movements
SELECT * FROM miovision_api.intersection_movements_denylist LIMIT 1;
*/