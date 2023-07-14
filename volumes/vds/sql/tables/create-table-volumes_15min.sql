--DROP TABLE vds.volumes_15min;

CREATE TABLE vds.volumes_15min (
    volumeid bigserial,
    detector_id text,
    division_id smallint,
    vds_id integer,
    num_lanes smallint,
    datetime_bin timestamp,
    volume_15min smallint,
    expected_bins smallint,
    num_obs smallint,
    num_distinct_lanes smallint,
    PRIMARY KEY (volumeid),
	UNIQUE (division_id, vds_id, datetime_bin)
);

ALTER TABLE vds.volumes_15min OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.volumes_15min TO vds_bot;

COMMENT ON TABLE vds.volumes_15min IS 'Table storing vehicle counts from `vds.raw_vdsdata` aggregated by detector / 15min bins.';