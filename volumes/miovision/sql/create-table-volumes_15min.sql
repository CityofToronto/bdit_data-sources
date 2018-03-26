DROP TABLE IF EXISTS miovision.volumes_15min;

CREATE TABLE miovision.volumes_15min (
	volume_15min_uid serial,
	intersection_uid integer,
	datetime_bin timestamp without time zone,
	classification_uid integer,
	leg text,
	dir text,
	volume numeric
	);