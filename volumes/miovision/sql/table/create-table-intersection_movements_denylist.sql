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

CREATE OR REPLACE TRIGGER intersection_movements_denylist_exclusion
BEFORE INSERT ON miovision_api.intersection_movements_denylist
FOR EACH ROW
EXECUTE FUNCTION miovision_api.intersection_movements_exclude();
