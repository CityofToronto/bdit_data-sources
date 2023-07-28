CREATE TABLE IF NOT EXISTS vds.raw_vdsvehicledata (
    volume_uid bigserial PRIMARY KEY,
    division_id smallint, 
    vds_id integer,
    dt timestamp,
    lane integer,
    sensor_occupancy_ds smallint,
    speed_kmh float,
    length_meter float,
    UNIQUE (division_id, vds_id, dt, lane)
); 

ALTER TABLE vds.raw_vdsvehicledata OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.raw_vdsvehicledata TO vds_bot;
GRANT ALL ON SEQUENCE vds.raw_vdsvehicledata_volume_uid_seq TO vds_bot;

COMMENT ON TABLE vds.raw_vdsvehicledata IS 'Store raw data pulled from ITS Central 
`vdsvehicledata` table. Filtered for divisionid = 2.'

-- DROP INDEX IF EXISTS vds.ix_vdsvehicledata_divid_dt;
CREATE INDEX IF NOT EXISTS ix_vdsvehicledata_divid_dt
ON vds.raw_vdsvehicledata
USING btree(
    division_id ASC nulls last,
    dt ASC nulls last
);

-- DROP INDEX IF EXISTS vds.ix_vdsvehicledata_divid_vdsid_dt;
CREATE INDEX IF NOT EXISTS ix_vdsvehicledata_divid_vdsid_dt
ON vds.raw_vdsvehicledata
USING btree(
    division_id ASC nulls last,
    vds_id ASC nulls last,
    dt ASC nulls last
);