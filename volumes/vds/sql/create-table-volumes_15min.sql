--DROP TABLE vds.volumes_15min;

CREATE TABLE vds.volumes_15min (
	volume_uid BIGSERIAL,
	detector_id text,
	divisionid smallint,
    vdsid integer,
	datetime_bin timestamp,
	volume_15min int,
	PRIMARY KEY (volume_uid),
  	UNIQUE (divisionid, vdsid, datetime_bin)
);

ALTER TABLE vds.volumes_15min OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.volumes_15min TO vds_bot;