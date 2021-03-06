-- trying to figure out how to split by points
WITH 
 pts AS (
	 SELECT * 
	 FROM 
	 (
	 -- get startpoints of each bylaw geometry
		SELECT o."ID"::NUMERIC bylaw_id, street_name streetname, extents, ST_SetSRID(geom::geometry, 26917) bylaw_geom, 
		ST_StartPoint(ST_SetSRID(geom::geometry, 26917)) pt, confidence,
		o."Speed_Limit_km_per_hr" speed_lim
        FROM crosic."posted_speed_limit_xml_open_data_withQC" p JOIN posted_speed_limit_xml_open_data_original o 
		ON p.street_name = o."Highway" AND p.extents = o."Between"
	 -- where ST_DWithin(ST_Transform(ST_SetSRID(ST_MakePoint(-79.386836, 43.704719), 4326), 26917), ST_Transform(geom::geometry, 26917) , 2000) 
	 ) x1
	
	 UNION 
	 -- get endpoints of bylaw geometry
	 SELECT o."ID"::NUMERIC bylaw_id, street_name, extents, ST_SetSRID(geom::geometry, 26917) bylaw_geom, 
			ST_EndPoint(ST_SetSRID(geom::geometry, 26917)) pt, confidence,
			o."Speed_Limit_km_per_hr" speed_lim
		FROM crosic."posted_speed_limit_xml_open_data_withQC" p JOIN posted_speed_limit_xml_open_data_original o 
	    ON p.street_name = o."Highway" AND p.extents = o."Between"
--	where  ST_DWithin(ST_Transform(ST_SetSRID(ST_MakePoint(-79.386836, 43.704719), 4326), 26917), ST_Transform(geom::geometry, 26917) , 2000) 
 	),
 gis AS (
	 -- streets gis layer with start points and end points of each centreline segment
	SELECT lf_name, objectid, ST_Transform(ST_LineMerge(geom), 26917) geom, ST_StartPoint(geom) centreline_start, ST_EndPoint(geom) centreline_end, fcode, fcode_desc
	 FROM gis.centreline 
	 WHERE fcode_desc IN ('Access Road', 'Collector', 'Collector Ramp','Expressway','Expressway Ramp','Laneway','Local','Major Arterial','Major Arterial Ramp','Minor Arterial','Minor Arterial Ramp','Other','Other Ramp','Pending')
	--and ST_DWithin(ST_Transform(ST_SetSRID(ST_MakePoint(-79.386836, 43.704719), 4326), 26917), ST_Transform(geom, 26917) , 2000) 
	 ),
	
  intersects_startpoint_endpoint AS (
		SELECT ST_LineLocatePoint(geom, ST_ClosestPoint(pt, geom)),
		ST_Transform(ST_ClosestPoint(pt, geom), 26917) pt,
		speed_lim, bylaw_geom, bylaw_id, streetname, extents, confidence
		FROM pts JOIN gis
		ON ST_DWithin(pt, geom, 1)
		WHERE NOT (ST_LineLocatePoint(geom, ST_ClosestPoint(pt, geom)) > 0.9815 OR ST_LineLocatePoint(geom, ST_ClosestPoint(pt, geom)) < 0.015) 
		AND ST_LineLocatePoint(geom, ST_ClosestPoint(pt, geom)) NOT IN (0, 1)
	)
,

broken_centreline_segments AS (
	SELECT DISTINCT ON (substr) * 
	FROM 
	(
		SELECT lf_name, objectid, ST_Transform(geom, 4326) geom, ST_Transform(bylaw_geom, 4326) bylaw_geom, 
		 substr, bylaw_id, (CASE WHEN len_int > 0 THEN speed_lim::int WHEN fcode_desc = 'Expressway' THEN 80 ELSE 50 END) AS speed_limit, 
		 fcode, fcode_desc, streetname, extents, confidence
			 FROM 
			(
			SELECT * 
			FROM 
				(
				SELECT 
				pt, geom, lf_name, objectid,  bylaw_id, speed_lim, 
					ST_Transform(ST_Line_Substring(geom, 0, ST_LineLocatePoint(geom, pt)), 4326) substr, 
					bylaw_geom, 
					ST_Length(ST_Intersection(ST_LineSubstring(geom, 0, ST_LineLocatePoint(geom, pt)), 
											  ST_Buffer(ST_LineSubString(bylaw_geom, 0.01, 0.99), 1, 'endcap=flat'))) len_int, 
					fcode, fcode_desc, streetname, extents, confidence
				FROM gis JOIN intersects_startpoint_endpoint
				ON ST_DWithin(pt, geom, 1)
				) x1

			UNION

				SELECT 
				pt, geom, lf_name,  objectid, bylaw_id, speed_lim, 
				ST_Transform(ST_Line_Substring(geom, ST_LineLocatePoint(geom, pt), 1), 4326) substr, 
				bylaw_geom,
				ST_Length(ST_Intersection(ST_LineSubstring(geom, ST_LineLocatePoint(geom, pt), 1) , 
										  ST_Buffer(ST_LineSubString(bylaw_geom, 0.01, 0.99), 1, 'endcap=flat'))) len_int, 
				fcode, fcode_desc, streetname, extents, confidence
				FROM gis JOIN intersects_startpoint_endpoint
				ON ST_DWithin(pt, geom, 1)
			) x

		WHERE ST_Length(substr) > 0
	) x1
	ORDER BY substr, (CASE WHEN speed_limit = 50 THEN 1000 ELSE speed_limit END)
  
 )
 ,
   
  -- get streets with speed limit changes from segments that were not broken 
 whole_segments_not50 AS (
  	SELECT DISTINCT ON (objectid) *
	  FROM gis, pts
	  WHERE objectid NOT IN (SELECT DISTINCT objectid FROM broken_centreline_segments)
  	  AND ST_Length(ST_Intersection(bylaw_geom, geom)) > 0
  ),


    -- get centreline segments that were not broken apart or get the other half of the segements that were (i.e. the 50km/hr half)
 whole_segments AS (
	SELECT * 
		 FROM gis 
		 WHERE objectid NOT IN 
		(SELECT DISTINCT objectid FROM broken_centreline_segments UNION SELECT DISTINCT objectid FROM whole_segments_not50)
	 )

	 -- put broken and non-broken centreline segments together
	-- WITH speed_limit_changes AS 


 SELECT DISTINCT ON(geom, lf_name)
	 x.*
	INTO crosic.tcl_speed_limit_aug292019
	 FROM 	 
	 (			
			SELECT lf_name, objectid, substr geom, fcode, fcode_desc, speed_limit, confidence, 
		    (CASE WHEN speed_limit = 50 THEN NULL ELSE bylaw_id END) bylaw_id, 
		    (CASE WHEN speed_limit = 50 THEN NULL ELSE streetname END) bylaw_streetname, 
		 	(CASE WHEN speed_limit = 50 THEN NULL ELSE extents END) bylaw_extents
			FROM broken_centreline_segments
			 
			UNION

			SELECT lf_name, objectid, ST_Transform(geom, 4326), fcode, fcode_desc,
		    (CASE WHEN fcode_desc = 'Expressway' THEN 80 ELSE 50 END) speed_limit, NULL,
		    NULL, NULL, NULL 
			FROM whole_segments
		 
		 	UNION 
		 
		 	SELECT lf_name, objectid,  ST_Transform(geom, 4326), fcode, fcode_desc, 
		    speed_lim::int speed_limit, confidence,
		    (CASE WHEN speed_lim::int = 50 THEN NULL ELSE bylaw_id END) bylaw_id, 
		    (CASE WHEN speed_lim::int = 50 THEN NULL ELSE streetname END) bylaw_streetname, 
		 	(CASE WHEN speed_lim::int = 50 THEN NULL ELSE extents END) bylaw_extents
		 	FROM whole_segments_not50
	 ) x LEFT JOIN pts USING(bylaw_id)
	 ORDER BY lf_name




