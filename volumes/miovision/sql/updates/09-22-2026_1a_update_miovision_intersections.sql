--- intersections + px + restrictions
UPDATE miovision_api.intersections
SET
    intersection_name = CASE intersection_uid
        WHEN 181 THEN 'Steeles / Barnac Dr'
        WHEN 166 THEN 'Steeles Ave W / 400 Ramp'
        WHEN 167 THEN 'Steeles / Irondale Dr'
        WHEN 168 THEN 'Steeles / Fenmar Dr'
        WHEN 174 THEN 'Steeles / Norfinch Dr'
        WHEN 175 THEN 'Steeles/ Peter Kaiser Gt'
        WHEN 176 THEN 'Steeles / Rossadean Dr'
        WHEN 177 THEN 'Steeles / Signet Dr'
        WHEN 178 THEN 'Steeles / Weston Rd'
        ELSE intersection_name
    END,
    px = CASE intersection_uid
        WHEN 181 THEN 1542
        WHEN 166 THEN 1487
        WHEN 167 THEN 1611
        WHEN 168 THEN 1209
        WHEN 174 THEN 1155
        WHEN 175 THEN 1511
        WHEN 176 THEN 1612
        WHEN 177 THEN 1232
        WHEN 178 THEN 1289
        ELSE px
    END;

--- date_installed

CREATE TEMP TABLE temp_min_dates AS
SELECT
    intersection_uid,
    MIN(datetime_bin)::date AS min_datetime
FROM miovision_api.volumes
JOIN miovision_api.intersections USING (intersection_uid)
WHERE
    WHERE intersection_uid IN (181, 166, 167, 168, 174, 175, 176, 177, 178)
GROUP BY intersection_uid;

UPDATE miovision_api.intersections AS i
SET date_installed = t.min_datetime
FROM temp_min_dates AS t
WHERE i.intersection_uid = t.intersection_uid;


--- traffic signal info

UPDATE miovision_api.intersections AS i
SET
    lat = ts.latitude,
    lng = ts.longitude,
    geom = ts.geom,
    street_main = ts.main_street,
    street_cross = ts.side1_street,
    int_id = ts.node_id,
    px = ts.px::integer
FROM gis.traffic_signal AS ts
WHERE i.px = ts.px::int
--set intersections here
AND i.intersection_uid IN (181, 166, 167, 168, 174, 175, 176, 177, 178);

--- DO NOT FORGET TO UPDATE THE GEOJSON (looking at you, sysadmin)
--cd ~/bdit_data-sources &&
--rm -f volumes/miovision/geojson/mio_intersections.geojson &&
--ogr2ogr -f "GeoJSON" volumes/miovision/geojson/mio_intersections.geojson PG:"host=trans-bdit-db-prod0-rds-smkrfjrhhbft.cpdcqisgj1fj.ca-central-1.rds.amazonaws.com dbname=bigdata" \
--	-sql "SELECT intersection_uid, intersection_name, date_installed, date_decommissioned, street_main, street_cross, int_id, px, n_leg_restricted, e_leg_restricted, s_leg_restricted, w_leg_restricted, geom
--	FROM miovision_api.intersections WHERE date_installed IS NOT NULL ORDER BY intersection_uid" -nln miovision_installations

--- restricted legs (The two highway ramps)
INSERT INTO miovision_api.intersection_movements_denylist (
    intersection_uid, classification_uid, movement_uid, leg
)
SELECT
    restricted_leg.intersection_uid::integer,
    c.classification_uid,
    m.movement_uid,
    restricted_leg.leg
FROM (VALUES (174, 'N'), (166, 'N'))
	AS restricted_leg (intersection_uid, leg),
UNNEST(ARRAY[1, 3, 4, 5, 9]) AS c (classification_uid),
UNNEST(ARRAY[1, 2, 3, 4]) AS m (movement_uid);

--- Intersection movements (untested)

--INSERT INTO miovision_api.intersection_movements (intersection_uid, classification_uid, leg, movement_uid)
--SELECT intersection_uid, classification_uid, leg, movement_uid
--FROM miovision_api.monitor_intersection_movements
--WHERE intersection_uid IN (181, 166, 167, 168, 174, 175, 176, 177, 178)
--ORDER BY intersection_uid, classification_uid, leg, movement_uid

--- Review intesrsection movements
--- Copy and run script here: https://github.com/CityofToronto/bdit_data-sources/tree/master/volumes/miovision/update_intersections#miovision_apiintersection_movements

--- Backfill Data
--- run shell script backfill_agg_tables-09222026.sh

--- QC Tables
--- Copy and run script here: https://github.com/CityofToronto/bdit_data-sources/tree/master/volumes/miovision/update_intersections#backfillaggregate-new-intersection-data