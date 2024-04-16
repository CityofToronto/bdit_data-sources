CREATE TABLE IF NOT EXISTS vds.raw_vdsvehicledata (
    division_id smallint NOT NULL,
    vds_id integer NOT NULL,
    dt timestamp without time zone NOT NULL,
    lane smallint NOT NULL,
    sensor_occupancy_ds smallint,
    speed_kmh double precision,
    length_meter double precision,
    volume_uid bigint NOT NULL DEFAULT nextval('vds.raw_vdsvehicledata_volume_uid_seq'::regclass),
    vdsconfig_uid integer REFERENCES vds.vdsconfig(uid),
    entity_location_uid integer REFERENCES vds.entity_locations(uid),
    CONSTRAINT raw_vdsvehicledata_pkey PRIMARY KEY (vds_id, dt, lane)
) PARTITION BY RANGE (dt);

ALTER TABLE vds.raw_vdsvehicledata OWNER TO vds_admins;
REVOKE ALL ON TABLE vds.raw_vdsvehicledata FROM vds_bot;
GRANT SELECT ON TABLE vds.raw_vdsvehicledata TO bdit_humans;
GRANT DELETE, INSERT, SELECT ON TABLE vds.raw_vdsvehicledata TO vds_bot;

COMMENT ON TABLE vds.raw_vdsvehicledata IS 'Store raw data pulled from ITS Central 
`vdsvehicledata` table. Filtered for divisionid = 2.';

--DROP INDEX IF EXISTS vds.ix_vdsvehicledata_dt;
CREATE INDEX IF NOT EXISTS ix_vdsvehicledata_dt
ON vds.raw_vdsvehicledata
USING brin(dt);

--DROP INDEX IF EXISTS vds.ix_vdsvehicledata_vdsid_dt;
CREATE INDEX IF NOT EXISTS ix_vdsvehicledata_vdsid_dt
ON vds.raw_vdsvehicledata
USING btree(
    vds_id ASC nulls last,
    dt ASC nulls last -- noqa: PRS
);

--DROP INDEX IF EXISTS vds.vdsvehicledata_volume_uid_idx;
--hoping to solve very slow delete from this large table.
CREATE INDEX IF NOT EXISTS vdsvehicledata_volume_uid_idx
ON vds.raw_vdsvehicledata
USING btree(volumeu_id ASC nulls last);

-- DROP INDEX IF EXISTS vds.ix_vdsvehicledata_vdsconfig_uid;
CREATE INDEX IF NOT EXISTS ix_vdsvehicledata_vdsconfig_uid
ON vds.raw_vdsvehicledata
USING btree(
    vdsconfig_uid ASC nulls last
);

-- DROP INDEX IF EXISTS vds.ix_vdsvehicledata_entity_location_uid;
CREATE INDEX IF NOT EXISTS ix_vdsvehicledata_entity_location_uid
ON vds.raw_vdsvehicledata
USING btree(
    entity_location_uid ASC nulls last
);

--Create yearly partitions, subpartition by month.
--new partitions created by vds_pull_vdsvehicledata DAG, `check_partitions` task.
SELECT vds.partition_vds_yyyymm('raw_vdsvehicledata', 2021, 'dt');
SELECT vds.partition_vds_yyyymm('raw_vdsvehicledata', 2022, 'dt');
SELECT vds.partition_vds_yyyymm('raw_vdsvehicledata', 2023, 'dt');

--add foreign keys referncing vdsconfig, entity_locations
CREATE TRIGGER add_vds_fkeys_vdsvehicledata
BEFORE INSERT ON vds.raw_vdsvehicledata
FOR EACH ROW
EXECUTE PROCEDURE vds.add_vds_fkeys();
