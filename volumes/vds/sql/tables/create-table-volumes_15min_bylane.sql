--DROP TABLE vds.volumes_15min;

CREATE TABLE vds.volumes_15min_bylane (
    volumeuid bigserial,
    detector_id text,
    division_id smallint,
    vds_id int,
    lane smallint,
    datetime_bin timestamp,
    volume_15min smallint,
    expected_bins smallint,
    num_obs smallint,
    PRIMARY KEY (volumeuid),
	UNIQUE (division_id, vds_id, lane, datetime_bin)
);

ALTER TABLE vds.volumes_15min_bylane OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.volumes_15min_bylane TO vds_bot;