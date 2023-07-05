--DROP TABLE vds.raw_vdsvehicledata; 

CREATE TABLE vds.raw_vdsvehicledata (
    division_id smallint, 
    vds_id integer,
    dt timestamp,
    lane integer,
    sensor_occupancy_ds smallint,
    speed_kmh float,
    length_meter float,
    PRIMARY KEY (division_id, vds_id, dt, lane)
); 

ALTER TABLE vds.raw_vdsvehicledata OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.raw_vdsvehicledata TO vds_bot;