--move some data for divisionid=8001 into a separate table and stop pulling 
    --into main table raw_vdsdata.
--this data is very sparse (65% empty rows)

CREATE TABLE IF NOT EXISTS vds.raw_vdsdata_div8001 (
    division_id smallint,
    vds_id integer,
    dt timestamp without time zone,
    datetime_15min timestamp without time zone,
    lane integer, 
    speed_kmh float, 
    volume_veh_per_hr integer,
    occupancy_percent float,
    PRIMARY KEY (division_id, vds_id, dt, lane)
); 

ALTER TABLE vds.raw_vdsdata OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.raw_vdsdata TO vds_bot;

INSERT INTO vds.raw_vdsdata_div8001
SELECT
    division_id,
    vds_id,
    dt,
    datetime_15min,
    lane,
    speed_kmh,
    volume_veh_per_hr,
    occupancy_percent
FROM vds.raw_vdsdata
WHERE division_id = 8001;

DELETE FROM vds.raw_vdsdata WHERE division_id = 8001;

COMMENT ON TABLE vds.raw_vdsdata_div8001 IS 'A one day sample of `vdsdata` for division_id = 8001 
from ITSC Central.';