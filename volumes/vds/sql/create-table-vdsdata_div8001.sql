--move some data for divisionid=8001 into a separate table and stop pulling into main table raw_vdsdata.
--this data is very sparse (65% empty rows)

--DROP TABLE vds.raw_vdsdata_div8001;

CREATE TABLE vds.raw_vdsdata_div8001 (
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

INSERT INTO vds.raw_vdsdata_div8001
SELECT
    rv.divisionid,
    rv.vdsid,
    rv.datetime_20sec,
    rv.datetime_15min,
    rv.lane,
    rv.speedkmh,
    rv.volumevehiclesperhour,
    rv.occupancypercent
FROM vds.raw_vdsdata AS rv
WHERE divisionid = 8001;

DELETE FROM vds.raw_vdsdata WHERE divisionid = 8001;
 
