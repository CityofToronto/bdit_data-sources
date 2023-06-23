--DROP TABLE gwolofs.raw_vdsvehicledata; 

CREATE TABLE gwolofs.raw_vdsvehicledata (
    divisionid smallint, 
    vdsid integer,
    timestamputc timestamp,
    lane integer,
    sensoroccupancyds smallint,
    speed_kmh float,
    length_meter float,
    PRIMARY KEY (divisionid, vdsid, timestamputc, lane)
); 

--ALTER TABLE gwolofs.raw_vdsvehicledata OWNER TO rescu_admins;