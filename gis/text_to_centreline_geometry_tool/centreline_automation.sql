CREATE OR REPLACE FUNCTION crosic.get_intersection_id(highway2 TEXT, btwn TEXT)
RETURNS INT AS $oid$
DECLARE 
oid INT;

BEGIN 
SELECT intersections.objectid
INTO oid
FROM 
(
	SELECT objectid,
		(
		CASE WHEN intersec5 LIKE '%/%'
		THEN TRIM(split_part(intersec5, '/', 1))
		ELSE intersec5
		END
		) AS intersec51, 
		(
		CASE WHEN intersec5 LIKE '%/%'
		THEN TRIM(split_part(intersec5, '/', 2))
		ELSE intersec5
		END
		) AS intersec52
	FROM gis.centreline_intersection
)
-- gis.centreline_intersection_streets 
AS intersections

WHERE levenshtein(TRIM(intersections.intersec51), btwn, 1, 1, 2) < 4  AND levenshtein(TRIM(intersections.intersec52), highway2, 1, 1, 2) < 4

-- select the intersection with the lowest combined levenshtien distance from the input streets
ORDER BY (levenshtein(intersections.intersec51, btwn, 1, 1, 2) +  levenshtein(intersections.intersec52, highway2, 1, 1, 2))
LIMIT 1;

RETURN oid; 

END; 
$oid$ LANGUAGE plpgsql; 



CREATE OR REPLACE FUNCTION crosic.get_intersection_geom(highway2 TEXT, btwn TEXT, direction TEXT, metres INT)
RETURNS GEOMETRY AS $geom$
DECLARE 
geom geometry;
oid INT := crosic.get_intersection_id(highway2, btwn);
oid_geom geometry := (
		SELECT ST_Transform(gis.geom, 26917)
		FROM gis.centreline_intersection gis
		WHERE objectid = oid
		);
BEGIN 
SELECT (
	-- normal case
	CASE WHEN direction IS NULL OR metres IS NULL 
	THEN oid_geom
	-- special case
	ELSE (
	(CASE WHEN direction = 'west' THEN ST_Translate(ST_Transform(oid_geom, 26917), -metres, 0) 
	WHEN direction = 'east' THEN ST_Translate(ST_Transform(oid_geom, 26917), metres, 0) 
	WHEN direction = 'north' THEN ST_Translate(ST_Transform(oid_geom, 26917), 0, metres) 
	WHEN direction = 'south' THEN ST_Translate(ST_Transform(oid_geom, 26917), 0, -metres) 
	END)
	)
	END 
) 
INTO geom;

RETURN geom; 

END; 
$geom$ LANGUAGE plpgsql; 





DROP FUNCTION crosic.get_line_geom(oid1_geom geometry, oid2_geom geometry);
CREATE OR REPLACE FUNCTION crosic.get_line_geom(oid1_geom geometry, oid2_geom geometry)
RETURNS GEOMETRY AS $geom$
DECLARE 
len INT := ST_LENGTH(ST_MakeLine(oid1_geom, oid2_geom)); 
geom GEOMETRY := 
	(
	-- WARNING INTEAD FOR LEN > 3000 ?????????
	CASE WHEN len > 11 AND len < 3000
	THEN ST_Transform(ST_MakeLine(oid1_geom, oid2_geom), 26917)
	END
	);
BEGIN

RETURN geom;

END;
$geom$ LANGUAGE plpgsql; 




DROP FUNCTION crosic.match_line_to_centreline(line_geom geometry, highway2 text, metres_btwn1 INT, metres_btwn2 INT);

CREATE OR REPLACE FUNCTION crosic.match_line_to_centreline(line_geom geometry, highway2 text, metres_btwn1 INT, metres_btwn2 INT)
RETURNS geometry AS $geom$
DECLARE geom geometry := (
	SELECT ST_Transform(ST_LineMerge(ST_UNION(s.geom)), 26917)
	FROM gis.centreline s
	WHERE levenshtein(LOWER(highway2), LOWER(s.lf_name), 1, 1, 2) < 3
	AND 
	(
	(
		-- "normal" case ... i.e. one intersection to another 
		COALESCE(metres_btwn1, metres_btwn2) IS NULL AND 
		ST_DWithin( ST_Transform(s.geom, 26917), ST_BUFFER(line_geom, 3*ST_LENGTH(line_geom), 'endcap=flat join=round') , 10)
		AND ST_Length(st_intersection(ST_BUFFER(line_geom, 3*(ST_LENGTH(line_geom)), 'endcap=flat join=round') , ST_Transform(s.geom, 26917))) /ST_Length(ST_Transform(s.geom, 26917)) > 0.9
	)
	OR 
	(
		-- 10 TIMES LENGTH WORKS .... LOOK INTO POTENTIALLY CHANGING LATER
		COALESCE(metres_btwn1, metres_btwn2) IS NOT NULL AND
		ST_DWithin(ST_Transform(s.geom, 26917), ST_BUFFER(line_geom, 10*COALESCE(metres_btwn1, metres_btwn2), 'endcap=flat join=round'), 30)
	)
	)
);

BEGIN 

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL; 




CREATE OR REPLACE FUNCTION crosic.centreline_case1(direction_btwn2 text, metres_btwn2 int, centreline_geom geometry, line_geom geometry, oid1_geom geometry, oid2_geom geometry)
RETURNS geometry AS $geom$

-- i.e. St Mark's Ave and a point 100 m north 

DECLARE geom geometry := (
-- case where the street from the intersection is shorter than x metres 
CASE WHEN metres_btwn2 > ST_Length(centreline_geom) AND metres_btwn2 - ST_Length(centreline_geom) < 15 
THEN centreline_geom


-- metres_btwn2/ST_Length(d.geom) is the fraction that is supposed to be cut off from the dissolved centreline segment(s)
-- cut off the first fraction of the dissolved line, and the second and check to see which one is closer to the original interseciton

WHEN ST_LineLocatePoint(centreline_geom, oid1_geom) 
> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
-- take the 
THEN ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid1_geom) - (metres_btwn2/ST_Length(centreline_geom)), 
ST_LineLocatePoint(centreline_geom, oid1_geom))


WHEN ST_LineLocatePoint(centreline_geom, oid1_geom) < 
ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
-- take the substring from the intersection to the point x metres ahead of it
THEN ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid1_geom), 
ST_LineLocatePoint(centreline_geom, oid1_geom) + (metres_btwn2/ST_Length(centreline_geom))  )


END
);

BEGIN 

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL; 









CREATE OR REPLACE FUNCTION crosic.centreline_case2(direction_btwn1 text, direction_btwn2 text, metres_btwn1 int, metres_btwn2 int, centreline_geom geometry, line_geom geometry, oid1_geom geometry, oid2_geom geometry)
RETURNS geometry AS $geom$


-- i.e. St Marks Ave and 100 metres north of St. John's Ave 


DECLARE 


geom geometry := (
	CASE WHEN (metres_btwn2 IS NOT NULL AND metres_btwn2 > ST_Length(centreline_geom) AND metres_btwn2 - ST_Length(centreline_geom) < 15)
	OR (metres_btwn1 IS NOT NULL AND metres_btwn1 > ST_Length(centreline_geom) AND metres_btwn1 - ST_Length(centreline_geom) < 15)
	THEN centreline_geom


	-- Case 1: direction_btwn1 IS NULL 
	-- find substring between intersection 1 and the point x metres away from intersection 2
	WHEN direction_btwn1 IS NULL
		THEN (
		-- when the intersection is after the rough line
		CASE WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		-- take the line from intersection 1 to x metres before intersection 2 
			THEN 
			(CASE WHEN  ST_LineLocatePoint(centreline_geom, oid1_geom)<   
			ST_LineLocatePoint(centreline_geom, oid2_geom) - (metres_btwn2/ST_Length(centreline_geom))
			THEN 
			ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid1_geom),  
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom)))
			ELSE 
			ST_LineSubstring(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom)), 
			ST_LineLocatePoint(centreline_geom, oid1_geom))
			END)

		-- when its before the closest point from rough line 
		WHEN  ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		-- take the line from intersection 1 to x metres after intersection 2
			THEN 
			(
			-- change order because of potential errors
			CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)<  
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom))
			THEN 
			ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid1_geom),  
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))
			ELSE 
			ST_LineSubstring(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)), 
			ST_LineLocatePoint(centreline_geom, oid1_geom))
			END )
		END
		)


	-- When direction 2 IS NULL 
	-- means that the zone is between interseciton 2 and x metres away from intersection 1
	WHEN direction_btwn2 IS NULL

		THEN (
		-- when the intersection is after the rough line
		CASE WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		-- take the line from intersection 2 to x metres before intersection 1
			THEN 
			(CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)) < ST_LineLocatePoint(centreline_geom, oid2_geom)
			THEN
			(CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)) > 0 
			THEN 
			ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, 
			oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom))
			ELSE 
			ST_LineSubstring(centreline_geom, 
			0,
			ST_LineLocatePoint(centreline_geom, oid2_geom))
			END)
			ELSE 
			ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid2_geom),
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)))

			END)

		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
			THEN 
			(CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)) > 1
			THEN ST_LineSubstring(centreline_geom,  
			ST_LineLocatePoint(centreline_geom, oid2_geom), 1)
			WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)) 
			< ST_LineLocatePoint(centreline_geom, oid2_geom)
			THEN 
			ST_LineSubstring(centreline_geom,  
			ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)), 
			ST_LineLocatePoint(centreline_geom, oid2_geom))
			ELSE 
			ST_LineSubstring(centreline_geom,  
			ST_LineLocatePoint(centreline_geom, oid2_geom), 
			ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)))

			END)


		END)


	-- when both are not null 
	-- the zone is between the point x metres away from int 1 and y metres away from int 2
	ELSE (
		-- both times the intersection occurs after (higher fraction) than the point closest to the end of the rough line 
		(CASE WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN 
			-- so line substring wont give error 
			(CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)) <
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom))
			THEN 
			ST_LineSubstring( centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom)))
			ELSE 
			ST_LineSubstring( centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)))
			END)


		-- both before 
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))


			THEN (
			-- so line substring wont give error
			CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)) <
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom))
			THEN 
			ST_LineSubstring( centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))
			ELSE 
			ST_LineSubstring( centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)), 
			ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)))

			END 
			)


		-- int 2 before (+), int 1 after (-)
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN 
			-- all of these cases are so the line_substring wont get mad ( 2nd arg must be smaller then 3rd arg) 
			(CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)) < 
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom))
			THEN ST_LineSubstring( centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))
			ELSE 
			ST_LineSubstring( centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)))
			END
			)


		-- int 2 before (+), int 1 after (-)
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN 
			-- so line substring wont give error
			(CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)) < 
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom))
			THEN 
			ST_LineSubstring( centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom)))
			ELSE 
			ST_LineSubstring( centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)))
			END)

		END
		) )



	END);



BEGIN 

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL; 







DROP FUNCTION IF EXISTS crosic.text_to_centreline(highway TEXT, frm TEXT, t TEXT) ;



CREATE OR REPLACE FUNCTION crosic.text_to_centreline(highway TEXT, frm TEXT, t TEXT) 
RETURNS geometry AS $centreline_segments$ 
DECLARE 


	-- clean data 


	
	/*
	-- when the input was btwn instead of from and to 
	
	btwn1 TEXT := gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(split_part(regexp_REPLACE(btwn, '[0123456789.]* metres (north|south|east|west|East) of ', '', 'g'), ' to ', 1), ' and ', 1), '\(.*\)', '', 'g'), 'Between ', '', 'g')); 

	btwn2_orig TEXT := CASE WHEN split_part(btwn, ' and ', 2) <> ''
			THEN gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(regexp_REPLACE(btwn, '[0123456789.]* metres (north|south|east|west|East) of ', '', 'g'), ' and ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g'))
			WHEN split_part(btwn, ' to ', 2) <> ''
			THEN gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(regexp_REPLACE(btwn, '[0123456789.]* metres (north|south|east|west|East) of ', '', 'g'), ' to ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g'))
			END; 

				
	highway2 TEXT :=  gis.abbr_street(regexp_REPLACE(highway, '\(.*\)', '', 'g'));
	direction_btwn1 TEXT := (
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(split_part(btwn, ' to ', 1), ' and ', 1), '\(.*\)', '', 'g'), 'Between ', '', 'g')) LIKE '% m %'
				THEN split_part(split_part(gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(split_part(btwn, ' to ', 1), ' and ', 1), '\(.*\)', '', 'g'), 'Between ', '', 'g')), ' m ', 2), ' of ', 1)
				ELSE NULL
				END );
	direction_btwn2 TEXT := (
				CASE WHEN btwn2_orig LIKE '% m %'
				OR 
				(
					CASE WHEN split_part(btwn, ' and ', 2) <> ''
					THEN gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(btwn, ' and ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g'))
					WHEN split_part(btwn, ' to ', 2) <> ''
					THEN gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(btwn, ' to ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g'))
					END
				) LIKE '% m %'
				THEN 
				(
					CASE WHEN split_part(btwn, ' and ', 2) <> ''
					THEN split_part(split_part( gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(btwn, ' and ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g')), ' m ', 2), ' of ', 1)
					WHEN split_part(btwn, ' to ', 2) <> ''
					THEN split_part(split_part(gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(btwn, ' to ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g')), ' m ', 2), ' of ', 1)
					END
				)
				ELSE NULL
				END);	
	metres_btwn1 INT :=	(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(split_part(btwn, ' to ', 1), ' and ', 1), '\(.*\)', '', 'g'), 'Between ', '', 'g')) LIKE '% m %'
				THEN regexp_REPLACE(split_part(gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(split_part(btwn, ' to ', 1), ' and ', 1), '\(.*\)', '', 'g'), 'Between ', '', 'g')), ' m ' ,1), 'a point ', '', 'g')::int
				ELSE NULL
				END
				);
	metres_btwn2 INT :=	( CASE WHEN btwn2_orig LIKE '% m %' OR 
				(
					CASE WHEN split_part(btwn, ' and ', 2) <> ''
					THEN gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(btwn, ' and ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g'))
					WHEN split_part(btwn, ' to ', 2) <> ''
					THEN gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(btwn, ' to ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g'))
					END
				) 
				LIKE '% m %'
				THEN 
				(
				CASE WHEN split_part(btwn, ' and ', 2) <> ''
				THEN regexp_REPLACE(split_part( gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(btwn, ' and ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g')), ' m ', 1), 'a point ', '', 'g')::int
				WHEN split_part(btwn, ' to ', 2) <> ''
				THEN regexp_REPLACE(split_part(gis.abbr_street( regexp_REPLACE(regexp_REPLACE(split_part(btwn, ' to ', 2), '\(.*\)', '', 'g'), 'Between ', '', 'g')), ' m ', 1), 'a point ', '', 'g')::int
				END
				)
				ELSE NULL
				END );
	*/ 
	

	btwn1 TEXT := gis.abbr_street(regexp_REPLACE(regexp_REPLACE(frm, '[0123456789.]* metres (north|south|east|west|East) of ', '', 'g'), '\(.*\)', '', 'g')); 

	btwn2_orig TEXT := gis.abbr_street(regexp_REPLACE(regexp_REPLACE(t, '[0123456789.]* metres (north|south|east|west|East) of ', '', 'g'), '\(.*\)', '', 'g')); 


	-- get rid of things within brackets 
	highway2 TEXT :=  gis.abbr_street(regexp_REPLACE(highway, '\(.*\)', '', 'g'));
	direction_btwn1 TEXT := (
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street(regexp_REPLACE(frm, '\(.*\)', '', 'g')) LIKE '% m %'
				THEN split_part(split_part(gis.abbr_street(regexp_REPLACE(frm, '\(.*\)', '', 'g')), ' m ', 2), ' of ', 1)
				ELSE NULL
				END );
	direction_btwn2 TEXT := (
				CASE WHEN btwn2_orig LIKE '% m %'
				OR gis.abbr_street(regexp_REPLACE(t, '\(.*\)', '', 'g')) LIKE '% m %'
				THEN 
				split_part(split_part(gis.abbr_street(regexp_REPLACE(t, '\(.*\)', '', 'g')), ' m ', 2), ' of ', 1)
				ELSE NULL
				END
				);	
				
	metres_btwn1 INT :=	(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street(regexp_REPLACE(frm, '\(.*\)', '', 'g')) LIKE '% m %'
				THEN regexp_REPLACE(split_part(gis.abbr_street(regexp_REPLACE(frm, '\(.*\)', '', 'g')), ' m ' ,1), 'a point ', '', 'g')::int
				ELSE NULL
				END
				);
				
	metres_btwn2 INT :=	( 
				CASE WHEN btwn2_orig LIKE '% m %' 
				OR gis.abbr_street(regexp_REPLACE(t, '\(.*\)', '', 'g')) LIKE '% m %'
				THEN 
				regexp_REPLACE(split_part(gis.abbr_street(regexp_REPLACE(t, '\(.*\)', '', 'g')), ' m ', 1), 'a point ', '', 'g')::int
				ELSE NULL
				END );

	
	-- for case one 
	-- i.e. Watson road from St. Mark's Road to a point 100 metres north
	-- we want the btwn2 to be St. Mark's Road (which is also btwn1)
	btwn2 TEXT := (
	CASE WHEN btwn2_orig LIKE '%point%'
	THEN btwn1
	ELSE btwn2_orig 
	END
	);

	-- get intersection geoms
	oid1_geom geometry := crosic.get_intersection_geom(highway2, btwn1, direction_btwn1, metres_btwn1);

	oid2_geom geometry := crosic.get_intersection_geom(highway2, btwn2, direction_btwn2, metres_btwn2);

	-- create a line between the two intersection geoms
	line_geom geometry = crosic.get_line_geom(oid1_geom, oid2_geom);

	-- match the lines to centreline segments
	centreline_segments geometry := ( 
				CASE WHEN COALESCE(metres_btwn1, metres_btwn2) IS NULL
				THEN 
				(
				SELECT * 
				FROM crosic.match_line_to_centreline(line_geom, highway2, metres_btwn1, metres_btwn2)
				)

				WHEN btwn1 = btwn2 
				THEN 
				(
				SELECT *  
				FROM crosic.centreline_case1(direction_btwn2, metres_btwn2, crosic.match_line_to_centreline(line_geom, highway2, metres_btwn1, metres_btwn2), line_geom, 
				crosic.get_intersection_geom(highway2, btwn1, NULL, NULL), crosic.get_intersection_geom(highway2, btwn2, NULL, NULL))
				)

				ELSE 
				(
				SELECT * 
				FROM crosic.centreline_case2(direction_btwn1, direction_btwn2, metres_btwn2, metres_btwn2, crosic.match_line_to_centreline(line_geom, highway2, metres_btwn1, metres_btwn2), line_geom, 
				-- get the original intersection geoms (not the translated ones) 
				crosic.get_intersection_geom(highway2, btwn1, NULL, NULL), crosic.get_intersection_geom(highway2, btwn2, NULL, NULL))
				)
				END
				);


BEGIN 

RETURN centreline_segments;


END;
$centreline_segments$ LANGUAGE plpgsql; 

-- $geom$





-- assumes highway_arr and btwn_arr are the same size
CREATE OR REPLACE FUNCTION crosic.make_geom_table(highway_arr TEXT[], frm_arr TEXT[], to_arr TEXT[]) 
RETURNS TABLE (
highway TEXT,
frm TEXT, 
t TEXT,
geom GEOMETRY
)
AS $$
BEGIN 

DROP TABLE IF EXISTS inputs;
CREATE TEMP TABLE inputs AS (
SELECT UNNEST(highway_arr) AS highway, UNNEST(frm_arr) AS frm, UNNEST(to_arr) AS t
);


RETURN QUERY  
SELECT i.highway, i.frm AS from, i.t AS to, crosic.text_to_centreline(i.highway, i.frm, i.t) AS geom
FROM inputs i;


END; 
$$ LANGUAGE plpgsql;


/*
-- old version with btwn instead of from and to
DROP TABLE IF EXISTS crosic.centreline_geoms_test;
SELECT * 
INTO crosic.centreline_geoms_test
FROM crosic.make_geom_table(ARRAY['Watson Avenue', 'Watson Avenue', 'Watson Avenue', 'North Queen Street'],
ARRAY['Between St Marks Road and St Johns Road', 'Between St Marks Road and a point 100 metres north', 'Between St Marks Road and 100 metres north of St Johns Road', 'Between Shorncliffe Road and Kipling Ave']);
*/ 


DROP TABLE IF EXISTS crosic.centreline_geoms_test;
SELECT * 
INTO crosic.centreline_geoms_test
FROM crosic.make_geom_table(ARRAY['Watson Avenue', 'Watson Avenue', 'Watson Avenue', 'North Queen Street'],
ARRAY['St Marks Road', 'St Marks Road', 'St Marks Road', 'Shorncliffe Road'], 
ARRAY['St Johns Road', 'a point 100 metres north', '100 metres north of St Johns Road', 'Kipling Ave'] );


-- SELECT crosic.text_to_centreline('Watson Avenue', 'Between St Marks Road and 100 metres north of St Johns Road');

-- SELECT crosic.text_to_centreline('Watson Avenue', 'Between St Marks Road and a point 100 metres north');

-- SELECT crosic.text_to_centreline('Watson Avenue', 'Between St Marks Road and St Johns Road');


-- works 
-- SELECT crosic.text_to_centreline('North Queen Street', 'Between Shorncliffe Road and Kipling Ave');