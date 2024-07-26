CREATE OR REPLACE FUNCTION miovision_api.exclude_intersection_movements_from_denylist()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $BODY$

DECLARE row_in_im numeric = (
    SELECT COUNT(*)
    FROM miovision_api.intersection_movements AS im
    WHERE
        im.intersection_uid = new.intersection_uid
        AND im.leg = new.leg
        AND im.movement_uid = new.movement_uid
        AND im.classification_uid = new.classification_uid
);

BEGIN
    IF (row_in_im) THEN
        RAISE NOTICE 'Row % is not being inserted. Already in intersection_movements table.', new;
        --Return null to stop insert. 
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;
END;
$BODY$;

ALTER FUNCTION miovision_api.exclude_intersection_movements_from_denylist OWNER TO miovision_admins;

GRANT EXECUTE ON miovision_api.exclude_intersection_movements_from_denylist TO miovision_api_bot;

COMMENT ON miovision_api.exclude_intersection_movements_from_denylist
IS 'Runs before insert into miovision_api.intersection_movements_denylist to prevent inserts
that are already in intersections_movements (allowlist).';

/*Test:
INSERT INTO miovision_api.intersection_movements_denylist
SELECT * FROM miovision_api.intersection_movements LIMIT 1;
*/