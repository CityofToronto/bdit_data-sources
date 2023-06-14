--DROP TABLE gwolofs.vds_rawdata; 

CREATE TABLE gwolofs.vds_rawdata (
    divisionid smallint,
    vdsid integer,
    datetime_20sec timestamp without time zone,
    datetime_15min timestamp without time zone,
    lane integer, 
    speedKmh float, 
    volumeVehiclesPerHour integer
    occupancyPercent float
    CONSTRAINT  PRIMARY KEY (divisionid, vdsid, datetime_20sec, lane)
); 

--ALTER TABLE gwolofs.vds_rawdata OWNER TO rescu_admins;