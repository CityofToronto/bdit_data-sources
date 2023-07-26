CREATE TABLE IF NOT EXISTS vds.counts_15min_bylane (
    volumeuid bigserial,
    detector_id text,
    division_id smallint,
    vds_id int,
    lane smallint,
    datetime_bin timestamp,
    count_15min smallint,
    expected_bins smallint,
    num_obs smallint,
    PRIMARY KEY (volumeuid),
    UNIQUE (division_id, vds_id, lane, datetime_bin)
);

ALTER TABLE vds.counts_15min_bylane OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.counts_15min_bylane TO vds_bot;
GRANT ALL ON SEQUENCE vds.counts_15min_bylane_volumeuid_seq TO vds_bot;

COMMENT ON TABLE vds.counts_15min_bylane IS 'Table storing vehicle counts from `vds.raw_vdsdata` 
aggregated by detector / lane / 15min bins.'