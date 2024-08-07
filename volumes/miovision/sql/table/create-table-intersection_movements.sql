CREATE TABLE IF NOT EXISTS miovision_api.intersection_movements (
    intersection_uid integer,
    classification_uid integer,
    leg text COLLATE pg_catalog."default",
    movement_uid integer,
    CONSTRAINT intersection_movements_new_intersection_uid_classification__key
    UNIQUE (intersection_uid, classification_uid, leg, movement_uid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.intersection_movements
OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.intersection_movements FROM bdit_humans;

GRANT TRIGGER, SELECT, REFERENCES ON TABLE miovision_api.intersection_movements
TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE miovision_api.intersection_movements TO miovision_admins;

COMMENT ON TABLE miovision_api.intersection_movements
IS 'Unique movements for each intersection by classification';

CREATE TRIGGER denylist_intersection_movements_exclusion
BEFORE INSERT ON miovision_api.intersection_movements
FOR EACH ROW
EXECUTE FUNCTION miovision_api.exclude_denylist_from_intersection_movements();
