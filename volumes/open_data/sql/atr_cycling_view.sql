-- uploading data from open data 


SELECT DISTINCT filename FROM uploaded_atr_bikes;

-- manually identify centreline_id 's
UPDATE uploaded_atr_bikes
SET centreline_id = 14068825
WHERE filename = 'Rogers Rd & Caledonia Rd';

UPDATE uploaded_atr_bikes
SET centreline_id = 8313231
WHERE filename = 'Adelaide St E & Jarvis St';

UPDATE uploaded_atr_bikes
SET centreline_id = 14036064
WHERE filename = 'Adelaide St W & Spadina St';


UPDATE uploaded_atr_bikes
SET centreline_id = 14047506
WHERE filename = 'Richmond St E & Jarvis St.';


UPDATE uploaded_atr_bikes
SET centreline_id = 7930588
WHERE filename= 'Richmond St W & Spadina St';


UPDATE uploaded_atr_bikes
SET centreline_id = 1143223
WHERE filename = 'Dupont St & Edwin St';

UPDATE uploaded_atr_bikes
SET centreline_id = 14014152
WHERE filename = 'Annette St & Keele St';


UPDATE uploaded_atr_bikes
SET centreline_id = 14072024
WHERE filename = 'Rogers Rd & Harvie St';





-- find values to insert into count_id 
-- assign count_id 's to the dates and locations 
INSERT INTO cycling.count_info(
SELECT DISTINCT ON (centreline_id, filename, direction, count_date) ROW_NUMBER () OVER (ORDER BY centreline_id) + 1083 AS count_id, 
centreline_id, (CASE WHEN direction IN ('EB', 'NB') THEN 1 ELSE -1 END) AS dir_id, direction dir, start_time::DATE AS count_date, daily_temperature AS temperature, daily_precipitation AS precipitation, NULL AS count_type, 
filename || ' 2018' AS filename, NULL AS location
FROM crosic.uploaded_atr_bikes
ORDER BY count_date
)


-- insert into count_data (only after inserting into count_id)
INSERT INTO cycling.count_data (
SELECT DISTINCT ON(count_id, start_time::TIME, end_time::TIME)count_id, start_time::TIME, end_time::TIME, volume
FROM crosic.uploaded_atr_bikes c LEFT JOIN cycling.count_info i 
ON c.centreline_id = i.centreline_id AND c.direction = i.dir AND count_date = start_time::DATE AND i.filename = c.filename || ' 2018'
WHERE count_id IS NOT NULL 
ORDER BY count_id
)



-- get centreline_id's that need to updated (i.e. were inputted incorrectly)
SELECT DISTINCT filename, centreline_id FROM cycling.count_info WHERE filename NOT IN (SELECT filename FROM cycling.count_data d JOIN cycling.count_info i ON
d.count_id = i.count_id 
JOIN gis.centreline ON geo_id = i.centreline_id
--LEFT JOIN directions di USING(centreline_id)
)


-- bayview and brickworks
UPDATE cycling.count_info i
SET centreline_id = 30099063
WHERE centreline_id = 30040115;

-- davenport and bedford
UPDATE cycling.count_info i
SET centreline_id = 11034642
WHERE centreline_id = 11034542;

-- Gatineau trail and warden
UPDATE cycling.count_info i
SET centreline_id = 30087960
WHERE centreline_id = 30020260;


-- Martin Goodman Trail Central 
UPDATE cycling.count_info i
SET centreline_id = 30021915
WHERE centreline_id = 30013094;


-- Martin Goodman Trail at coronation park
UPDATE cycling.count_info i
SET centreline_id = 30102160
WHERE centreline_id = 30013094;




-- Martin Goodman Trail at defunct 
UPDATE cycling.count_info i
SET centreline_id = 30070140
WHERE centreline_id = 30013094;


-- Martin Goodman Trail at lake shore and strachan 
UPDATE cycling.count_info i
SET centreline_id = 30102161
WHERE centreline_id = 30013094;


-- Martin Goodman Trail at lake shore and strachan 
UPDATE cycling.count_info i
SET centreline_id = 30102161
WHERE centreline_id = 12106;



SELECT DISTINCT filename, regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
 regexp_replace(filename, 'btw Yonge & Church', 'AT ' || 'Yonge and Church' ), '&', 'AND'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'AT')), ' - CYCLIST COUNT', '') 
FROM cycling.count_info





DROP TABLE IF EXISTS all_street_names; 
CREATE TEMP TABLE all_street_names AS 
(
SELECT DISTINCT 
CASE
WHEN split_part(filename, ' btw ', 2) <> ''
THEN 'WELLESLEY'
WHEN split_part(filename, ' @ ', 2) = ''
THEN split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
filename, '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', '')), ' - CYCLIST COUNT', ''), ' & ', 1)
ELSE 
split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
filename, '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', '')), ' - CYCLIST COUNT', ''), ' @ ', 1)
END AS street1, 
CASE WHEN split_part(filename, ' btw ', 2) <> ''
THEN 'YONGE'
WHEN split_part(filename, ' @ ', 2) = ''
THEN split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
filename, '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', '')), ' - CYCLIST COUNT', ''), ' & ', 2)
ELSE 
split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
filename, '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', '')), ' - CYCLIST COUNT', ''), ' @ ', 2)
END AS street2, 
lf_name,
centreline_id, geom
FROM cycling.count_data d JOIN cycling.count_info i ON
d.count_id = i.count_id 
JOIN gis.centreline ON geo_id = i.centreline_id
); 



DROP TABLE IF EXISTS street_names;
CREATE TEMP TABLE street_names AS 
(
SELECT
CASE WHEN levenshtein(street1, UPPER(lf_name)) < levenshtein(street2, UPPER(lf_name))
THEN street2
ELSE 
street1
END as street1, 
lf_name as street2, 
centreline_id, geom
FROM all_street_names
); 







DROP TABLE IF EXISTS possible_intersections;
CREATE TEMP TABLE possible_intersections AS (
SELECT s.*, intersec5, objectid
FROM street_names s JOIN gis.centreline_intersection i
ON ST_Intersects(ST_Buffer(ST_Centroid(ST_SetSRID(ST_Transform(s.geom, 26717), 26717)), 250), ST_SetSRID(ST_Transform(i.geom, 26717), 26717))
ORDER BY s.street1, s.street2
);


DROP TABLE IF EXISTS lev_check;
WITH intersections AS 
(
SELECT DISTINCT ON(centreline_id, street1, street2) objectid, centreline_id, street1, street2
FROM 
(
SELECT intersections.objectid, centreline_id, street1, street2
FROM gis.centreline_intersection_streets AS intersections, 
street_names sn
WHERE (levenshtein(TRIM(UPPER(street)), TRIM(UPPER(street1)), 1, 1, 2) < 8 OR levenshtein(TRIM(UPPER(street)), TRIM(UPPER(street2)), 1, 1, 2) < 8)
-- AND centreline_id = 14021174
AND objectid IN (SELECT objectid FROM possible_intersections ps WHERE ps.centreline_id = sn.centreline_id AND ps.street1 = sn.street1 AND ps.street2 = sn.street2) 
AND (street1 <> '' AND street2 <> '')
GROUP BY intersections.objectid, centreline_id, street1, street2
HAVING COUNT(intersections.street) > 1
ORDER BY centreline_id, street1, street2, AVG(LEAST(levenshtein(TRIM(UPPER(street)), TRIM(UPPER(street1)), 1, 1, 2), levenshtein(TRIM(UPPER(street)), TRIM(UPPER(street2)), 1, 1, 2)))
) x
)


SELECT i.centreline_id, i.street1, i.street2, i.objectid, intersec5
INTO TEMPORARY TABLE lev_check
FROM street_names s JOIN intersections i 
USING(centreline_id)
JOIN gis.centreline_intersection USING(objectid);


-- supposed to be bay and bloor
UPDATE lev_check 
SET objectid = 19459
WHERE centreline_id = 8408494;


-- supposed to be yonge and dundas
UPDATE lev_check 
SET objectid = 18401
WHERE centreline_id = 7762758 OR centreline_id = 9879826;


-- supposed to king and John 
UPDATE lev_check 
SET objectid = 12254
WHERE centreline_id = 20139610;


UPDATE lev_check 
SET objectid = 5653 
WHERE centreline_id = 1140250;

INSERT INTO lev_check (centreline_id, street1, street2, objectid)
VALUES(30072436, 'MARTIN GOODMAN TRAIL', 'SIMCOE', 13012),
--(30029567, 'HARRISON ESTATES PARK', '', 87617), 
--(30054691, 'ETIENNE BRULE PARK', '', 98168), 
--(30006614, 'MARTIN GOODMAN TRAIL WEST', '', 42103)
(7753909, 'JARVIS', 'WELLESLEY', 17041), 
(14024688, 'JARVIS', 'WELLESLEY', 17041),
(14072024, 'ROGERS', 'HARVIE', 8224)
;




/*

-- check to make sure that the assigned tables work 
SELECT * FROM all_street_names a1, all_street_names a2 WHERE (a1.street1 <> a2.street1 OR a2.street2 <> a1.street2) AND a2.centreline_id = a1.centreline_id 

-- should only be one exception + records where either street1 or street2 is null 
SELECT DISTINCT centreline_id, street1, street2 
FROM  all_street_names
WHERE centreline_id NOT IN (SELECT centreline_id FROM lev_check)

*/ 


DROP TABLE IF EXISTS directions;
-- get direction 
SELECT centreline_id, i.objectid, 
CASE WHEN degrees(ST_Azimuth(i.geom, ST_Centroid(gis.geom))) < 45 OR degrees(ST_Azimuth(i.geom, ST_Centroid(gis.geom))) > 315
THEN 'N'
WHEN degrees(ST_Azimuth(i.geom, ST_Centroid(gis.geom))) > 45 AND degrees(ST_Azimuth(i.geom, ST_Centroid(gis.geom))) < 135
THEN 'E'
WHEN degrees(ST_Azimuth(i.geom, ST_Centroid(gis.geom))) > 135 AND degrees(ST_Azimuth(i.geom, ST_Centroid(gis.geom))) < 225 
THEN 'S'
ELSE 'W'
END AS dir, 
degrees(ST_Azimuth(i.geom, ST_Centroid(gis.geom))) AS degrees
INTO TEMPORARY TABLE directions
FROM lev_check lc JOIN centreline_intersection i
USING(objectid) 
JOIN gis.centreline gis ON geo_id = lc.centreline_id
ORDER BY centreline_id;





ALTER TABLE cycling.count_info ADD COLUMN location text; 


UPDATE cycling.count_info i
SET location = location_finder.location
FROM 
(
SELECT DISTINCT ON(centreline_id, i.dir, location) i.centreline_id, i.dir, 

CASE WHEN split_part(
regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
regexp_replace(filename, 'btw Yonge & Church', ' OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''), ' OF ', 2) <> ''

THEN regexp_replace(regexp_replace(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
regexp_replace(filename, 'btw Yonge & Church', 'OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''), 
split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
regexp_replace(filename, 'btw Yonge & Church', 'OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''), ' OF ', 1), 
UPPER(lf_name) || ' ' || i.dir  || ' ' || di.dir), 

split_part(regexp_replace(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
regexp_replace(filename, 'btw Yonge & Church', 'OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''), 
split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
regexp_replace(filename, 'btw Yonge & Church', 'OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''), ' OF ', 1), 
								(
								SELECT UPPER(TRIM(street))
								FROM gis.centreline_intersection_streets ci 
								WHERE ci.objectid = di.objectid
								ORDER BY levenshtein(UPPER(TRIM(street)), 
											UPPER(TRIM(lf_name)),
											1, 1, 2)
											
								LIMIT 1
	) || ' ' || i.dir  || ' ' || di.dir), ' OF ', 2) ,


	 (
				SELECT UPPER(TRIM(street))
				FROM gis.centreline_intersection_streets ci 
				WHERE ci.objectid = di.objectid
				ORDER BY 
				CASE WHEN levenshtein(UPPER(TRIM(lf_name)), 
				split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
				regexp_replace(filename, 'btw Yonge & Church', 'OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''),
				' OF ', 2), 1, 1, 2) > 	levenshtein(UPPER(TRIM(lf_name)), 
				split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
				regexp_replace(filename, 'btw Yonge & Church', 'OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''),
				' OF ', 1), 1, 1, 2)
				THEN levenshtein(UPPER(TRIM(street)), 
				split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
				regexp_replace(filename, 'btw Yonge & Church', 'OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''),
				' OF ', 2), 1, 1, 2)
				ELSE
				levenshtein(UPPER(TRIM(street)), 
				split_part(regexp_replace(UPPER(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
				regexp_replace(filename, 'btw Yonge & Church', 'OF ' || 'Yonge and Church' ), '&', 'OF'), '.xlsx', ''), ' 2015', ''), ' 2016', ''), ' TMC', ''), '.xls', ''), ' 2017', ''), ' 2014', ''), ' 2013', ''), '@', 'OF')), ' - CYCLIST COUNT', ''),
				' OF ', 1), 1, 1, 2)
				END
					
				LIMIT 1
				)
) 


ELSE 
UPPER(lf_name) || ' ' || i.dir

END
AS location 

FROM
cycling.count_data d JOIN cycling.count_info i ON
d.count_id = i.count_id 
JOIN gis.centreline ON geo_id = i.centreline_id
LEFT JOIN directions di USING(centreline_id)
WHERE i.location IS NULL 

) AS location_finder
WHERE i.location IS NULL and i.centreline_id = location_finder.centreline_id AND i.dir = location_finder.dir ;




-- brickworks is not a street it is a farmers market thing so the name could not match to an intersection 
UPDATE cycling.count_info
SET location = 'BAYVIEW ST' || ' ' || dir || ' S OF EVERGREEN BRICK WORKS' 
WHERE centreline_id = 30099063 











DROP VIEW IF EXISTS  open_data.atr_shortterm_cycling_volumes  ;
CREATE VIEW open_data.volumes_atr_shortterm_cycling
AS (


SELECT i.centreline_id, i.dir direction, location, 
temperature daily_temperature, precipitation daily_percipitation, 
(SELECT (count_date || ' ' || start_time)::TIMESTAMP) datetime_bin_start, (SELECT (count_date || ' ' || end_time)::TIMESTAMP) datetime_bin_end, volume

FROM
cycling.count_data d JOIN cycling.count_info i ON
d.count_id = i.count_id 
);



