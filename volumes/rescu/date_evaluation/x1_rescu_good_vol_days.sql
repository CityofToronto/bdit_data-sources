-- See how many days have RESCU volume counts that meet daily volume thresholds based on the thresholds in rescu_enuf_vol.sql

CREATE TABLE scannon.rescu_good_vol_days AS (
    SELECT 
        ev.detector_id,
        di.det_group,
        di.number_of_lanes,
        di.primary_road || ' and ' || di.cross_road AS gen_loc,
        ST_SETSRID(ST_MakePoint(di.longitude, di.latitude), 4326) AS geom,
        COUNT(ev.dt) AS ev_day_ct 
    FROM scannon.rescu_enuf_vol AS ev
    LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
    GROUP BY 
        ev.detector_id,
        di.det_group,
        di.number_of_lanes,
        di.primary_road || ' and ' || di.cross_road,
        ST_SETSRID(ST_MakePoint(di.longitude, di.latitude), 4326)
    HAVING COUNT(ev.dt) >= 100
    ORDER BY di.det_group, ev.detector_id
);