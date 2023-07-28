CREATE TABLE IF NOT EXISTS vds.counts_15min (
    volumeuid bigserial,
    detector_id text,
    division_id smallint,
    vds_id integer,
    num_lanes smallint,
    datetime_15min timestamp,
    count_15min smallint,
    expected_bins smallint,
    num_obs smallint,
    num_distinct_lanes smallint,
    PRIMARY KEY (volumeuid),
    UNIQUE (division_id, vds_id, datetime_15min)
);

ALTER TABLE vds.counts_15min OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.counts_15min TO vds_bot;
GRANT ALL ON SEQUENCE vds.counts_15min_volumeid_seq TO vds_bot;

COMMENT ON TABLE vds.counts_15min IS 'Table storing vehicle counts from `vds.raw_vdsdata` 
aggregated by detector / 15min bins.';

-- DROP INDEX IF EXISTS vds.ix_counts15_divid_dt;
CREATE INDEX IF NOT EXISTS ix_counts15_divid_dt
ON vds.counts_15min
USING btree (
    division_id ASC NULLS LAST,
    datetime_15min ASC NULLS LAST
);

-- DROP INDEX IF EXISTS vds.ix_counts15_divid_vdsid_dt;
CREATE INDEX IF NOT EXISTS ix_counts15_divid_vdsid_dt
ON vds.counts_15min
USING btree (
    division_id ASC NULLS LAST,
    vds_id ASC NULLS LAST,
    datetime_15min ASC NULLS LAST
);