DROP TABLE rescu.volumes_15min;

CREATE TABLE rescu.volumes_15min (
	volume_uid serial not null,
	detector_id text,
	datetime_bin timestamp without time zone,
	volume_15min int,
	arterycode int
	);
	
ALTER TABLE rescu.volumes_15min OWNER TO rescu_admins;