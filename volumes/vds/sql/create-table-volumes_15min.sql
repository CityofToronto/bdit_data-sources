--DROP TABLE vds.volumes_15min;

CREATE TABLE vds.volumes_15min (
    volume_uid bigserial,
    detector_id text,
    division_id smallint,
    vds_id integer,
    datetime_bin timestamp,
    volume_15min int,
    PRIMARY KEY (volume_uid),
	UNIQUE (division_id, vds_id, datetime_bin)
);

ALTER TABLE vds.volumes_15min OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.volumes_15min TO vds_bot;