CREATE TABLE IF NOT EXISTS vds.counts_15min_bylane
(
    volumeuid bigint NOT NULL DEFAULT nextval('vds.counts_15min_bylane_volumeuid_seq'::regclass),
    division_id smallint NOT NULL,
    vdsconfig_uid integer REFERENCES vds.vdsconfig(uid),
    entity_location_uid integer REFERENCES vds.entity_locations(uid),
    lane smallint NOT NULL,
    datetime_15min timestamp without time zone NOT NULL,
    count_15min smallint,
    expected_bins smallint,
    num_obs smallint,
    CONSTRAINT counts_15min_bylane_partitioned_pkey PRIMARY KEY (division_id, vdsconfig_uid, lane, datetime_15min)
) PARTITION BY LIST (division_id);

ALTER TABLE vds.counts_15min_bylane OWNER TO vds_admins;
REVOKE ALL ON TABLE vds.counts_15min_bylane FROM vds_bot;
GRANT SELECT ON TABLE vds.counts_15min_bylane TO bdit_humans;
GRANT ALL ON TABLE vds.counts_15min_bylane TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.counts_15min_bylane TO vds_bot;

COMMENT ON TABLE vds.counts_15min_bylane IS '''Table storing vehicle counts from `vds.raw_vdsdata` 
ggregated by detector / lane / 15min bins.''';
-- DROP INDEX IF EXISTS vds.ix_counts15_divid_dt;

CREATE INDEX IF NOT EXISTS ix_counts15_bylane_dt
ON vds.counts_15min_bylane
USING brin(datetime_15min);

-- DROP INDEX IF EXISTS vds.ix_counts15_bylane_vdsconfiguid_dt;
CREATE INDEX IF NOT EXISTS ix_counts15_bylane_vdsconfiguid_dt
ON vds.counts_15min_bylane
USING btree(
    vdsconfig_uid ASC nulls last,
    datetime_15min ASC nulls last -- noqa: PRS
);

-- DROP INDEX IF EXISTS vds.ix_counts15_entity_location_uid;
CREATE INDEX IF NOT EXISTS ix_counts15_bylane_entity_location_uid
ON vds.counts_15min_bylane
USING btree(
    entity_location_uid ASC nulls last
);

--create partition for div 2. Subpartition by date. 
--Removed div 8001 partition since these detectors only have 1 lane (don't need by lane summary).
CREATE TABLE vds.counts_15min_bylane_div2
PARTITION OF vds.counts_15min_bylane FOR VALUES IN ('2')
PARTITION BY RANGE (datetime_15min);

ALTER TABLE IF EXISTS vds.counts_15min_bylane_div2 OWNER TO vds_admins;

-- create sub partitions by year.
SELECT vds.partition_vds_yyyy('counts_15min_bylane_div2', 2021);
SELECT vds.partition_vds_yyyy('counts_15min_bylane_div2', 2022);
SELECT vds.partition_vds_yyyy('counts_15min_bylane_div2', 2023);