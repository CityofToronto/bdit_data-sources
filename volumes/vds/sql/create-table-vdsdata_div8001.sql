--move some data for divisionid=8001 into a separate table and stop pulling into main table raw_vdsdata.
--this data is very sparse (65% empty rows)

--DROP TABLE vds.raw_vdsdata_div8001;

CREATE TABLE vds.raw_vdsdata_div8001 (
    division_id smallint,
    vds_id integer,
    datetime_20sec timestamp without time zone,
    datetime_15min timestamp without time zone,
    lane integer, 
    speed_kmh float, 
    volume_veh_per_hr integer,
    occupancy_percent float,
    PRIMARY KEY (division_id, vds_id, datetime_20sec, lane)
); 

ALTER TABLE vds.raw_vdsdata OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.raw_vdsdata TO vds_bot;

INSERT INTO vds.raw_vdsdata_div8001
SELECT
    rv.division_id,
    rv.vds_id,
    rv.datetime_20sec,
    rv.datetime_15min,
    rv.lane,
    rv.speed_kmh,
    rv.volume_veh_per_hour,
    rv.occupancy_percent
FROM vds.raw_vdsdata AS rv
WHERE division_id = 8001;

DELETE FROM vds.raw_vdsdata WHERE division_id = 8001;
 
