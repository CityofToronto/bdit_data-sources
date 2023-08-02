CREATE TABLE IF NOT EXISTS vds.counts_15min (
    volumeuid bigint NOT NULL DEFAULT nextval('vds.counts_15min_volumeuid_seq'::regclass),
    detector_id text,
    division_id smallint NOT NULL,
    vds_id integer NOT NULL,
    num_lanes smallint,
    datetime_15min timestamp without time zone NOT NULL,
    count_15min smallint,
    expected_bins smallint,
    num_obs smallint,
    num_distinct_lanes smallint,
    CONSTRAINT counts_15min_partitioned_pkey PRIMARY KEY (division_id, vds_id, datetime_15min)
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

-- DROP INDEX IF EXISTS vds.ix_counts15_vdsid_dt;
CREATE INDEX IF NOT EXISTS ix_counts15_vdsid_dt
ON vds.counts_15min
USING btree(
    vds_id ASC nulls last,
    datetime_15min ASC nulls last
);

--Create partitions by division_id. Subpartition by date.
CREATE TABLE vds.counts_15min_div2
PARTITION OF vds.counts_15min FOR VALUES IN ('2') 
PARTITION BY RANGE (datetime_15min);
ALTER TABLE IF EXISTS vds.counts_15min_div2 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div8001
PARTITION OF vds.counts_15min FOR VALUES IN ('8001')
PARTITION BY RANGE (datetime_15min);
ALTER TABLE IF EXISTS vds.counts_15min_div8001 OWNER TO vds_admins;

--Division 2 subpartitions
CREATE TABLE vds.counts_15min_div2_2017
PARTITION OF vds.counts_15min_div2 FOR VALUES FROM ('2017-01-01 00:00:00') TO ('2018-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div2_2017 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div2_2018
PARTITION OF vds.counts_15min_div2 FOR VALUES FROM ('2018-01-01 00:00:00') TO ('2019-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div2_2018 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div2_2019
PARTITION OF vds.counts_15min_div2 FOR VALUES FROM ('2019-01-01 00:00:00') TO ('2020-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div2_2019 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div2_2020
PARTITION OF vds.counts_15min_div2 FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2021-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div2_2020 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div2_2021
PARTITION OF vds.counts_15min_div2 FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2022-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div2_2021 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div2_2022
PARTITION OF vds.counts_15min_div2 FOR VALUES FROM ('2022-01-01 00:00:00') TO ('2023-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div2_2022 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div2_2023
PARTITION OF vds.counts_15min_div2 FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2024-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div2_2023 OWNER TO vds_admins;

--Division 8001 subpartitions
CREATE TABLE vds.counts_15min_div8001_2021
PARTITION OF vds.counts_15min_div8001 FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2022-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div8001_2021 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div8001_2022
PARTITION OF vds.counts_15min_div8001 FOR VALUES FROM ('2022-01-01 00:00:00') TO ('2023-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div8001_2022 OWNER TO vds_admins;

CREATE TABLE vds.counts_15min_div8001_2023
PARTITION OF vds.counts_15min_div8001 FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2024-01-01 00:00:00');
ALTER TABLE IF EXISTS vds.counts_15min_div8001_2023 OWNER TO vds_admins;