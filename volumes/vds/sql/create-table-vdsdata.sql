--DROP TABLE gwolofs.raw_vdsdata; 

CREATE TABLE gwolofs.raw_vdsdata (
    divisionid smallint,
    vdsid integer,
    datetime_20sec timestamp without time zone,
    datetime_15min timestamp without time zone,
    lane integer, 
    speedKmh float, 
    volumeVehiclesPerHour integer
    occupancyPercent float,
    PRIMARY KEY (divisionid, vdsid, datetime_20sec, lane)
); 

--ALTER TABLE gwolofs.raw_vdsdata OWNER TO rescu_admins;