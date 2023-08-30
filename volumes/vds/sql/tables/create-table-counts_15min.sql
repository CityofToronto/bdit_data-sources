CREATE TABLE IF NOT EXISTS vds.counts_15min (
    volumeuid bigint NOT NULL DEFAULT nextval('vds.counts_15min_volumeuid_seq'::regclass),
    division_id smallint NOT NULL,
    vdsconfig_uid integer REFERENCES vds.vdsconfig(uid),
    entity_location_uid integer REFERENCES vds.entity_locations(uid),
    num_lanes smallint,
    datetime_15min timestamp without time zone NOT NULL,
    count_15min smallint,
    expected_bins smallint,
    num_obs smallint,
    num_distinct_lanes smallint,
    CONSTRAINT counts_15min_partitioned_pkey PRIMARY KEY (division_id, vdsconfig_uid, datetime_15min)
) PARTITION BY LIST (division_id);

ALTER TABLE vds.counts_15min OWNER TO vds_admins;
REVOKE ALL ON TABLE vds.counts_15min FROM vds_bot;
GRANT SELECT ON TABLE vds.counts_15min TO bdit_humans;
GRANT ALL ON TABLE vds.counts_15min TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.counts_15min TO vds_bot;

COMMENT ON TABLE vds.counts_15min IS 'Table storing vehicle counts from `vds.raw_vdsdata` 
aggregated by detector / 15min bins.';

-- DROP INDEX IF EXISTS vds.ix_counts15_dt;
CREATE INDEX IF NOT EXISTS ix_counts15_dt
ON vds.counts_15min
USING brin(datetime_15min);

-- DROP INDEX IF EXISTS vds.ix_counts15_vdsconfiguid_dt;
CREATE INDEX IF NOT EXISTS ix_counts15_vdsconfiguid_dt
ON vds.counts_15min
USING btree(
    vdsconfig_uid ASC nulls last,
    datetime_15min ASC nulls last -- noqa: PRS
);

-- DROP INDEX IF EXISTS vds.counts_15_volumeuid_idx;
-- hoping to solve very slow delete from this large table.
CREATE INDEX IF NOT EXISTS counts_15_volumeuid_idx
ON vds.counts_15min
USING btree(volumeuid ASC nulls last);

-- DROP INDEX IF EXISTS vds.ix_counts15_entity_location_uid;
CREATE INDEX IF NOT EXISTS ix_counts15_entity_location_uid
ON vds.counts_15min
USING btree(
    entity_location_uid ASC nulls last
);

--Create partitions by division_id. Subpartition by date.
CREATE TABLE vds.counts_15min_div2
PARTITION OF vds.counts_15min FOR VALUES IN ('2') 
PARTITION BY RANGE (datetime_15min);
ALTER TABLE IF EXISTS vds.counts_15min_div2 OWNER TO vds_admins;

--Removed partition for div 8001 and turned into a view with same name: vds.counts_15min_div8001
--CREATE TABLE vds.counts_15min_div8001
--PARTITION OF vds.counts_15min FOR VALUES IN ('8001')
--PARTITION BY RANGE (datetime_15min);
--ALTER TABLE IF EXISTS vds.counts_15min_div8001 OWNER TO vds_admins;

--Division 2 subpartitions
--Sub partitions created with vds.partition_vds_yyyy
--new partitions created by vds_pull_vdsdata DAG, `check_partitions` task.
SELECT vds.partition_vds_yyyy('counts_15min_div2', 2017);
SELECT vds.partition_vds_yyyy('counts_15min_div2', 2018);
SELECT vds.partition_vds_yyyy('counts_15min_div2', 2019);
SELECT vds.partition_vds_yyyy('counts_15min_div2', 2020);
SELECT vds.partition_vds_yyyy('counts_15min_div2', 2021);
SELECT vds.partition_vds_yyyy('counts_15min_div2', 2022);
SELECT vds.partition_vds_yyyy('counts_15min_div2', 2023);