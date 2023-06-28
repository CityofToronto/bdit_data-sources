--DROP TABLE vds.raw_vdsdata;

CREATE TABLE vds.raw_vdsdata (
    divisionid smallint,
    vdsid integer,
    datetime_20sec timestamp without time zone,
    datetime_15min timestamp without time zone,
    lane integer, 
    speedKmh float, 
    volumeVehiclesPerHour integer,
    occupancyPercent float,
    PRIMARY KEY (divisionid, vdsid, datetime_20sec, lane)
); 

ALTER TABLE vds.raw_vdsdata OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.raw_vdsdata TO vds_bot;