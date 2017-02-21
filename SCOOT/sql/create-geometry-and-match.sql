DROP TABLE IF EXISTS px_tcl CASCADE;

CREATE TEMPORARY TABLE px_tcl (px int, centreline_id bigint, shape geometry, point geometry, sideofint text, sec int);

INSERT INTO px_tcl(px,centreline_id, shape, point)
SELECT px, centreline_id,
	(CASE WHEN from_intersection_id = node_id THEN shape
	ELSE ST_Reverse(shape) END) AS shape, 
	(CASE WHEN from_intersection_id = node_id THEN ST_StartPoint(shape)
	ELSE ST_EndPoint(shape) END) AS point

FROM prj_volume.px 
	JOIN (SELECT centreline_id, shape, from_intersection_id, to_intersection_id 
	      FROM prj_volume.centreline 
	      WHERE feature_code_desc NOT IN ('Geostatistical line', 'Hydro Line','Creek/Tributary','Major Railway','Major Shoreline','Minor Shoreline (Land locked)','Busway','River','Walkway','Ferry Route','Trail')
	     ) AS c
	ON (node_id = from_intersection_id OR node_id = to_intersection_id)
WHERE px IN (SELECT DISTINCT px FROM prj_volume.scoot_detectors)
ORDER BY px;

INSERT INTO px_tcl(px, point)
SELECT px, ST_Transform(ST_SetSRID(ST_MakePoint(Longitude,Latitude),4326),82181) AS point
FROM prj_volume.px 
WHERE px IN (SELECT DISTINCT px FROM prj_volume.scoot_detectors) AND px NOT IN (SELECT DISTINCT px FROM px_tcl);
	
UPDATE px_tcl
SET (centreline_id, shape) = 
	(SELECT DISTINCT ON (A.px) B.centreline_id, B.shape
	FROM px_tcl A JOIN prj_volume.centreline B ON ST_DWithin(point,B.shape,8)
	WHERE A.px = px_tcl.px 
	ORDER BY A.px, ST_DISTANCE(point,B.shape))
WHERE centreline_id IS NULL;

UPDATE px_tcl 
SET sec = FLOOR((ST_Azimuth(point, ST_EndPoint(shape)) + 0.292)  / (pi()/4));

UPDATE px_tcl
SET sideofint = 
	(CASE 
		WHEN sec in (0,7,8) THEN 'N'
		WHEN sec in (1,2) THEN 'E'
		WHEN sec in (3,4) THEN 'S'
		WHEN sec in (5,6) THEN 'W'
	END)
WHERE px IN (SELECT px FROM prj_volume.px WHERE Node_ID != 0 and Node_ID IS NOT NULL);

UPDATE px_tcl
SET sideofint = 
	(CASE 
		WHEN sec in (0,3,4,7,8) THEN 'N'
		WHEN sec in (1,2,5,6) THEN 'E'
	END)
WHERE px IN (SELECT px FROM prj_volume.px WHERE Node_ID = 0 or Node_ID IS NULL);

INSERT INTO px_tcl(px,centreline_id, shape, point,sideofint,sec)
SELECT px, centreline_id, shape, point, 
	(CASE 
		WHEN sec in (0,3,4,7,8) THEN 'S'
		WHEN sec in (1,2,5,6) THEN 'W'
	END) as sideofint, sec
FROM px_tcl
WHERE px IN (SELECT px FROM prj_volume.px WHERE Node_ID = 0 or Node_ID IS NULL);


ALTER TABLE px_tcl DROP COLUMN sec;

DROP TABLE IF EXISTS duplicate_to_keep;
CREATE TEMPORARY TABLE duplicate_to_keep (px int, centreline_id bigint, shape geometry, point geometry, sideofint text);

INSERT INTO duplicate_to_keep
SELECT C.px, B.centreline_id, B.shape, B.point, C.sideofint
FROM prj_volume.px A INNER JOIN px_tcl B USING (px) INNER JOIN 
	(SELECT px, sideofint
	FROM px_tcl
	GROUP BY px,sideofint
	HAVING count(*) > 1) C USING (px,sideofint) INNER JOIN
	prj_volume.centreline D USING (centreline_id)
WHERE UPPER(D.linear_name_full) = A.Main or UPPER(D.linear_name_full)=A.Side1 or UPPER(D.linear_name_full)=A.Side2;

DELETE FROM duplicate_to_keep
WHERE px IN (SELECT px FROM duplicate_to_keep GROUP BY px HAVING count(*)>1);

DELETE FROM px_tcl
WHERE (px,sideofint) IN (SELECT px,sideofint FROM duplicate_to_keep);

INSERT INTO px_tcl
SELECT *
FROM duplicate_to_keep;

UPDATE px_tcl
SET sideofint = 'S'
WHERE px = 209 AND centreline_id=30057507;

UPDATE px_tcl
SET sideofint = 'S'
WHERE px = 222 AND centreline_id=30006295;

UPDATE px_tcl
SET sideofint = 'N'
WHERE px = 230 AND centreline_id=13297428;

UPDATE px_tcl
SET sideofint = 'S'
WHERE px = 230 AND centreline_id=14672915;

UPDATE px_tcl
SET sideofint = 'E'
WHERE px = 230 AND centreline_id=30070016;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px = 230 AND centreline_id=30008185;

UPDATE px_tcl
SET sideofint = 'S'
WHERE px = 295 AND centreline_id=8571207;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px = 378 AND centreline_id=111123;

UPDATE px_tcl
SET sideofint = 'E'
WHERE px = 378 AND centreline_id=111053;

UPDATE px_tcl
SET sideofint = 'N'
WHERE px = 455 AND centreline_id=444013;

UPDATE px_tcl
SET sideofint = 'E'
WHERE px = 508 AND centreline_id=13971133;

UPDATE px_tcl
SET sideofint = 'N'
WHERE px = 566 AND centreline_id=913231;

DELETE FROM px_tcl
WHERE px = 747 AND centreline_id=438115;

DELETE FROM px_tcl
WHERE px = 786 AND centreline_id=12377329;

UPDATE px_tcl
SET sideofint = 'S'
WHERE px = 883 AND centreline_id=913169;

UPDATE px_tcl
SET sideofint = 'S'
WHERE px = 954 AND centreline_id=909738;

DELETE FROM px_tcl
WHERE px = 1185 AND centreline_id=436123;

DELETE FROM px_tcl
WHERE px = 1262 AND centreline_id=436109;

UPDATE px_tcl
SET sideofint = 'E'
WHERE px = 1420 AND centreline_id=7878;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px = 1420 AND centreline_id=7206707;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px = 1421 AND centreline_id=8097;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px = 1541 AND centreline_id=1146604;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px = 1650 AND centreline_id=30019690;

UPDATE px_tcl
SET sideofint = 'N'
WHERE px = 1918 AND centreline_id=30013783;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px = 820 AND centreline_id=9212691;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px = 823 AND centreline_id=446666;

UPDATE px_tcl
SET sideofint = 'S'
WHERE px = 823 AND centreline_id=5625788;

INSERT INTO px_tcl (px,centreline_id,sideofint,point,shape)
SELECT B.px, A.centreline_id, 'E' AS sideofint, C.point, A.shape
FROM (prj_volume.centreline A CROSS JOIN prj_volume.px B) JOIN px_tcl C USING (PX)
WHERE B.px=867 AND A.centreline_id=7754954
LIMIT 1;

UPDATE px_tcl
SET sideofint = 'W'
WHERE px=896 AND centreline_id=7979413;

INSERT INTO px_tcl (px,centreline_id,sideofint,point,shape)
SELECT B.px, A.centreline_id, 'W' AS sideofint, C.point, A.shape
FROM (prj_volume.centreline A CROSS JOIN prj_volume.px B) JOIN px_tcl C USING (PX)
WHERE B.px=1559 AND A.centreline_id=8396807
LIMIT 1;

INSERT INTO px_tcl (px,centreline_id,sideofint,point,shape)
SELECT B.px, A.centreline_id, 'E' AS sideofint, C.point, A.shape
FROM (prj_volume.centreline A CROSS JOIN prj_volume.px B) JOIN px_tcl C USING (PX)
WHERE B.px=1559 AND A.centreline_id=9212691
LIMIT 1;

DELETE FROM px_tcl
WHERE px=1559 and sideofint='S';

UPDATE px_tcl
SET centreline_id = 1145533
WHERE px=1559 AND sideofint='N';

UPDATE px_tcl
SET sideofint = 'S'
WHERE px = 1871 AND centreline_id=14257769;

SELECT px,sideofint
FROM px_tcl
GROUP BY px, sideofint
HAVING COUNT(*) > 1
ORDER BY px;
