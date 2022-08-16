--IDENTIFY COORDINATES ISSUES

---------------------------------------------------------------------------------------------------------------------------------------------------
--INSIDE/OUTSIDE BOUNDARY COLLISIONS
---------------------------------------------------------------------------------------------------------------------------------------------------
--Table inserted from Python to postgres rbahreh.collisions_2010_pointInPolys 

SELECT COUNT(DISTINCT collision_no) FROM rbahreh."collisions_2010_pointInPolys";--611273

--ALTER TABLE rbahreh.col_orig_abbr ADD COLUMN inside_city
ALTER TABLE rbahreh.col_orig_abbr ADD COLUMN inside_city BOOLEAN;

UPDATE rbahreh.col_orig_abbr
SET inside_city = TRUE
WHERE collision_no in (SELECT distinct collision_no from rbahreh."collisions_2010_pointInPolys" );

UPDATE rbahreh.col_orig_abbr
SET inside_city = False
WHERE inside_city is null;--7889
---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------
--NULL COORDINATES
---------------------------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM rbahreh.col_orig_abbr WHERE longitude is null or latitude is null;--15

UPDATE rbahreh.col_orig_abbr
SET coord_issue = 'null_coord'
WHERE longitude is null or latitude is null;
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--ZERO COORDINATES
---------------------------------------------------------------------------------------------------------------------------------------------------
--ALTER TABLE rbahreh.col_orig_abbr ADD COLUMN inside_city
ALTER TABLE rbahreh.col_orig_abbr ADD COLUMN coord_issue text;

--ZERO COORD
SELECT * FROM rbahreh.col_orig_abbr WHERE inside_city = False AND (longitude > -5 OR latitude < 5);--1917

UPDATE rbahreh.col_orig_abbr
SET coord_issue = 'Zero_coord'
WHERE inside_city = False AND (longitude > -5 OR Latitude < 5);
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--CRC Coord (York)
---------------------------------------------------------------------------------------------------------------------------------------------------
--**NOTE:** The YORK CRC coordinates was identified using an exploration in python. CODES can be find in the same notebook
SELECT * FROM rbahreh.col_orig_abbr WHERE longitude::numeric = -79.551245 AND latitude = 43.756516;

UPDATE rbahreh.col_orig_abbr
SET coord_issue = 'York_coord'
WHERE inside_city = TRUE AND (longitude::numeric = -79.551245 AND latitude = 43.756516);
---------------------------------------------------------------------------------------------------------------------------------------------------