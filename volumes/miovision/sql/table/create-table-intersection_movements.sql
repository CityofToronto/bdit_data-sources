SELECT DISTINCT intersection_uid, classification_uid, leg, movement_uid
	INTO miovision.intersection_movements
	FROM miovision.volumes_15min_mvt;
	ALTER TABLE miovision.intersection_movements ADD UNIQUE (intersection_uid, classification_uid, leg, movement_uid);
	COMMENT ON TABLE miovision.intersection_movements IS 'Unique movements for each intersection by classification';
	GRANT ALL ON TABLE miovision.intersection_movements TO miovision_admins;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision.intersection_movements TO bdit_humans WITH GRANT OPTION;

CREATE TRIGGER denylist_intersection_movements_exclusion
BEFORE INSERT ON miovision_api.intersection_movements
FOR EACH ROW
EXECUTE FUNCTION miovision_api.exclude_denylist_from_intersection_movements();