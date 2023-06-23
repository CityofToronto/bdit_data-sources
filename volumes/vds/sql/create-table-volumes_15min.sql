--DROP TABLE gwolofs.vds_volumes_15min;

CREATE TABLE gwolofs.vds_volumes_15min (
	volume_uid serial not null,
	detector_id text,
	divisionid smallint,
    vdsid integer,
	datetime_bin timestamp,
	volume_15min int
	);

--ALTER TABLE gwolofs.vds_volumes_15min OWNER TO rescu_admins;