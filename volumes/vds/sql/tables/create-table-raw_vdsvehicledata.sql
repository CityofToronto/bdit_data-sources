--DROP TABLE vds.raw_vdsvehicledata; 

CREATE TABLE vds.raw_vdsvehicledata (
    volume_uid bigserial,
    division_id smallint, 
    vds_id integer,
    dt timestamp,
    lane integer,
    sensor_occupancy_ds smallint,
    speed_kmh float,
    length_meter float,
    PRIMARY KEY volume_uid,
    UNIQUE (division_id, vds_id, dt, lane)
); 

ALTER TABLE vds.raw_vdsvehicledata OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.raw_vdsvehicledata TO vds_bot;

COMMENT ON TABLE vds.raw_vdsvehicledata IS 'Store raw data pulled from ITS Central `vdsvehicledata` table. Filtered for divisionid = 2.'