CREATE TABLE IF NOT EXISTS vds.raw_vdsdata (
    volume_uid bigserial,
    division_id smallint,
    vds_id integer,
    dt timestamp without time zone,
    datetime_15min timestamp without time zone,
    lane integer, 
    speed_kmh float, 
    volume_veh_per_hr integer,
    occupancy_percent float,
    PRIMARY KEY volume_uid, 
    UNIQUE (division_id, vds_id, dt, lane)
); 

ALTER TABLE vds.raw_vdsdata OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.raw_vdsdata TO vds_bot;
GRANT ALL ON SEQUENCE vds.raw_vdsdata_volume_uid_seq TO vds_bot;

COMMENT ON TABLE vds.raw_vdsdata IS 'Store raw data pulled from ITS Central 
`vdsdata` table. Filtered for divisionid = 2.';

-- DROP INDEX IF EXISTS vds.ix_vdsdata_vdsid_dt;
CREATE INDEX IF NOT EXISTS ix_vdsdata_vdsid_dt
    ON vds.raw_vdsdata USING btree
    (division_id ASC NULLS LAST, dt ASC NULLS LAST);
    
-- DROP INDEX IF EXISTS vds.ix_vdsdata_divid_vdsid_dt;
CREATE INDEX IF NOT EXISTS ix_vdsdata_divid_vdsid_dt
    ON vds.raw_vdsdata USING btree
    (division_id ASC NULLS LAST, vds_id ASC NULLS LAST, dt ASC NULLS LAST);