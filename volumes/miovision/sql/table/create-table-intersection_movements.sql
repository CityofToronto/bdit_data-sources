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
TO bdit_humans;

GRANT ALL ON TABLE miovision_api.intersection_movements TO miovision_admins;

COMMENT ON TABLE miovision_api.intersection_movements
IS 'Unique movements for each intersection by classification';

--prevent insertion of movements that are in denylist
CREATE OR REPLACE TRIGGER intersection_movements_denylist_exclusion
BEFORE INSERT ON miovision_api.intersection_movements
FOR EACH ROW
EXECUTE FUNCTION miovision_api.intersection_movements_exclude();

ALTER TABLE IF EXISTS miovision_api.intersection_movements
ADD CONSTRAINT intersecton_movements_exclude_bike_exits
CHECK (NOT (classification_uid = 10 AND movement_uid = 8));

ALTER TABLE IF EXISTS miovision_api.intersection_movements
ADD CONSTRAINT intersecton_movements_exclude_xwalk_bikes
CHECK (NOT (classification_uid = 7));

--insert padding values into volumes_15min_mvt_unfiltered
CREATE TRIGGER miovision_intersection_movements_pad
AFTER INSERT ON miovision_api.intersection_movements
FOR EACH ROW
EXECUTE FUNCTION miovision_api.fn_add_intersection_movement_padding_values();

--insert other vehicle modes besides light autos upon insert of light autos
CREATE OR REPLACE TRIGGER miovision_intersection_movements_insert_other_modes
AFTER INSERT
ON miovision_api.intersection_movements
FOR EACH ROW
EXECUTE FUNCTION miovision_api.intersection_movements_insert_other_modes();
