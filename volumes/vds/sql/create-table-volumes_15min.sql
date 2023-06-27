--DROP TABLE gwolofs.vds_volumes_15min;

CREATE TABLE gwolofs.vds_volumes_15min (
	volume_uid BIGSERIAL,
	--detector_id text, #remove for now before inventory is brought in. 
	divisionid smallint,
    vdsid integer,
	datetime_bin timestamp,
	volume_15min int
	PRIMARY KEY (volume_uid),
  	UNIQUE (divisionid, vdsid, datetime_bin)
);

--ALTER TABLE gwolofs.vds_volumes_15min OWNER TO rescu_admins;