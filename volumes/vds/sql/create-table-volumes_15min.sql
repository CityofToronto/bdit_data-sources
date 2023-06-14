--DROP TABLE gwolofs.vds_volumes_15min;

CREATE TABLE gwolofs.vds_volumes_15min (
	volume_uid serial not null,
	detector_id text,
	datetime_bin timestamp without time zone,
	volume_15min int,
	arterycode int
	);

--ALTER TABLE gwolofs.vds_volumes_15min OWNER TO rescu_admins;