CREATE TABLE IF NOT EXISTS miovision_api.intersection_movements_denylist (
    intersection_uid integer,
    classification_uid integer,
    leg text COLLATE pg_catalog."default",
    movement_uid integer,
    CONSTRAINT intersection_movements_denylist_intersection_uid_classification__key
    UNIQUE (intersection_uid, classification_uid, leg, movement_uid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.intersection_movements_denylist
OWNER TO miovision_admins;
GRANT ALL ON TABLE miovision_api.intersection_movements_denylist TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.intersection_movements_denylist FROM bdit_humans;

GRANT TRIGGER, SELECT, REFERENCES ON TABLE miovision_api.intersection_movements_denylist
TO bdit_humans WITH GRANT OPTION;

COMMENT ON TABLE miovision_api.intersection_movements_denylist
IS 'Unique movements which we do not want to aggregate.
Uniqueness from intersection_movements is enforced via on insert trigger.';

CREATE TRIGGER intersection_movements_denylist_exclusion
BEFORE INSERT ON miovision_api.intersection_movements_denylist
FOR EACH ROW
EXECUTE FUNCTION miovision_api.exclude_intersection_movements_from_denylist();

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

TRUNCATE miovision_api.intersection_movements_denylist;

INSERT INTO miovision_api.intersection_movements_denylist
SELECT * FROM miovision_api.intersection_movements LIMIT 1;

INSERT INTO miovision_api.intersection_movements_denylist (intersection_uid, classification_uid, leg, movement_uid)
VALUES (1,	2, 'E',	3);