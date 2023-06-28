--DROP TABLE vds.raw_vdsvehicledata; 

CREATE TABLE vds.raw_vdsvehicledata (
    divisionid smallint, 
    vdsid integer,
    timestamputc timestamp,
    lane integer,
    sensoroccupancyds smallint,
    speed_kmh float,
    length_meter float,
    PRIMARY KEY (divisionid, vdsid, timestamputc, lane)
); 

ALTER TABLE vds.raw_vdsvehicledata OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.raw_vdsvehicledata TO vds_bot;