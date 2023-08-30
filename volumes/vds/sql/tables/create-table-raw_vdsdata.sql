CREATE TABLE IF NOT EXISTS vds.raw_vdsdata
(
    division_id smallint NOT NULL,
    vds_id integer NOT NULL,
    dt timestamp without time zone NOT NULL,
    datetime_15min timestamp without time zone,
    lane integer NOT NULL,
    speed_kmh double precision,
    volume_veh_per_hr integer,
    occupancy_percent double precision,
    volume_uid bigint NOT NULL DEFAULT nextval('vds.raw_vdsdata_volume_uid_seq'::regclass),
    vdsconfig_uid integer REFERENCES vds.vdsconfig(uid),
    entity_location_uid integer REFERENCES vds.entity_locations(uid),
    CONSTRAINT raw_vdsdata_unique PRIMARY KEY (division_id, vds_id, dt, lane)
) PARTITION BY LIST (division_id);

ALTER TABLE IF EXISTS vds.raw_vdsdata OWNER TO vds_admins;
REVOKE ALL ON TABLE vds.raw_vdsdata FROM vds_bot;
GRANT SELECT ON TABLE vds.raw_vdsdata TO bdit_humans;
GRANT ALL ON TABLE vds.raw_vdsdata TO vds_admins;
GRANT DELETE, INSERT, SELECT ON TABLE vds.raw_vdsdata TO vds_bot;
GRANT ALL ON SEQUENCE vds.raw_vdsdata_volume_uid_seq TO vds_bot;

COMMENT ON TABLE vds.raw_vdsdata IS 'Store raw data pulled from ITS Central `vdsdata` table.';
   
-- DROP INDEX IF EXISTS vds.ix_vdsdata_vdsid_dt;
CREATE INDEX IF NOT EXISTS ix_vdsdata_vdsid_dt
ON vds.raw_vdsdata
USING btree(
    vds_id ASC nulls last,
    dt ASC nulls last -- noqa: PRS
);

-- DROP INDEX IF EXISTS vds.ix_vdsdata_dt;
CREATE INDEX IF NOT EXISTS ix_vdsdata_dt
ON vds.raw_vdsdata
USING brin(dt);

-- DROP INDEX IF EXISTS vds.volume_uid_idx;
CREATE INDEX IF NOT EXISTS volume_uid_idx
ON vds.raw_vdsdata
USING btree(volume_uid ASC nulls last);

-- DROP INDEX IF EXISTS vds.ix_vdsdata_vdsconfig_uid;
CREATE INDEX IF NOT EXISTS ix_vdsdata_vdsconfig_uid
ON vds.raw_vdsdata
USING btree(
    vdsconfig_uid ASC nulls last
);

-- DROP INDEX IF EXISTS vds.ix_vdsdata_entity_location_uid;
CREATE INDEX IF NOT EXISTS ix_vdsdata_entity_location_uid
ON vds.raw_vdsdata
USING btree(
    entity_location_uid ASC nulls last
);

--Partition for division_id = 2. Subpartition by date (year). 
CREATE TABLE vds.raw_vdsdata_div2 PARTITION OF vds.raw_vdsdata
FOR VALUES IN (2)
PARTITION BY RANGE (dt);
ALTER TABLE IF EXISTS vds.raw_vdsdata_div2 OWNER TO vds_admins;

--Partition for division_id = 8001. Subpartition by date (year). 
CREATE TABLE vds.raw_vdsdata_div8001 PARTITION OF vds.raw_vdsdata
FOR VALUES IN (8001)
PARTITION BY RANGE (dt);
ALTER TABLE IF EXISTS vds.raw_vdsdata_div8001 OWNER TO vds_admins;

--Sub partitions created with vds.partition_vds_yyyymm
--new partitions created by vds_pull_vdsdata DAG, `check_partitions` task.
SELECT vds.partition_vds_yyyymm('raw_vdsdata_div2', 2021, 'dt');
SELECT vds.partition_vds_yyyymm('raw_vdsdata_div2', 2022, 'dt');
SELECT vds.partition_vds_yyyymm('raw_vdsdata_div2', 2023, 'dt');
SELECT vds.partition_vds_yyyymm('raw_vdsdata_div8001', 2021, 'dt');
SELECT vds.partition_vds_yyyymm('raw_vdsdata_div8001', 2022, 'dt');
SELECT vds.partition_vds_yyyymm('raw_vdsdata_div8001', 2023, 'dt');

--add foreign keys referncing vdsconfig, entity_locations
CREATE TRIGGER add_vds_fkeys_vdsdata
BEFORE INSERT ON vds.raw_vdsdata
FOR EACH ROW
EXECUTE PROCEDURE vds.add_vds_fkeys();