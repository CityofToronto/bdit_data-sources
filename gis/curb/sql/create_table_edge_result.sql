
CREATE TABLE gis.curb_all_edges AS

WITH clean_geom AS (    
    SELECT _id AS int_id,
    "SUBTYPE_DESC" AS polytype,
	-- St_exteriorring to get rid of interior rings (medians) that we dont care about
	-- st_geomtryN that grab only 1 geom from multopolygon which there are only 32 in this layer,
	-- aka small edge case that we are not dealing with
    ST_ExteriorRing(ST_GeometryN(ST_Transform(geom, 2952), 1)) AS geom
FROM natalie.edge_of_roadway)

-- grab all vertices from each polygon except the last one
-- which is a duplicate of the first point
, pts AS (
    SELECT int_id, polytype, pt_no, pt_geom, total_points
    FROM (
        SELECT
            int_id,
            polytype,
            (ST_DumpPoints(geom)).path[1] - 1 AS pt_no,
            (ST_DumpPoints(geom)).geom AS pt_geom,
            ST_NPoints(geom) - 1 AS total_points
        FROM clean_geom
    ) all_points
    WHERE pt_no < total_points
),
-- find previous and next pt for each vertices
neighbour_nos AS (
    SELECT
        int_id, polytype, pt_no, pt_geom, total_points,
        CASE WHEN pt_no = 0
             THEN total_points - 1
             ELSE pt_no - 1
        END AS prev_pt_no,
        CASE WHEN pt_no = total_points - 1
             THEN 0
             ELSE pt_no + 1
        END AS next_pt_no
    FROM pts
),

azimuths AS (
    SELECT
        int_id, polytype, pt_no,
        DEGREES(ST_Azimuth(prev_geom, this_geom)) AS az_in_deg,
        DEGREES(ST_Azimuth(this_geom, next_geom)) AS az_out_deg
    FROM (
			    SELECT
			        v.int_id,
			        v.polytype,
			        v.pt_no,
			        prev.pt_geom AS prev_geom,
			        v.pt_geom    AS this_geom,
			        nxt.pt_geom  AS next_geom
			    FROM neighbour_nos v
			    JOIN pts prev ON prev.int_id = v.int_id AND prev.pt_no = v.prev_pt_no
			    JOIN pts nxt  ON nxt.int_id  = v.int_id AND nxt.pt_no  = v.next_pt_no
			)neighbour_pts
),

-- identify turning points aka where there is a sharp turn using
-- the previous and next point and calculating the azimuth
-- choose 50 degrees because that seem to be a good number
-- after some trial and error
corners AS (
    SELECT
        int_id, polytype, pt_no
    FROM (
    SELECT
        int_id, polytype, pt_no,
        ABS(az_out_deg - az_in_deg) AS raw_diff_deg
    FROM azimuths) angle_diffs
    WHERE LEAST(raw_diff_deg, 360 - raw_diff_deg) > 50
),

-- find the start and end point for each line that we 
-- need to cut/reconstruct 
cuts AS (
    SELECT
        int_id, polytype, pt_no AS start_id,
        COALESCE(LEAD(pt_no) OVER w, FIRST_VALUE(pt_no) OVER w) AS end_id
    FROM corners
    WINDOW w AS (PARTITION BY int_id ORDER BY pt_no)
),

segments AS (
    SELECT
	-- make distinct line_id based on int_id and start and end pt number
        c.int_id::text || '_' || c.start_id || '_' || c.end_id AS line_id,  
        c.int_id, c.polytype, c.start_id, c.end_id,
		-- make line in order of verticies placement 
        ST_MakeLine(
            p.pt_geom
            ORDER BY CASE WHEN p.pt_no >= c.start_id THEN p.pt_no
                          ELSE p.pt_no + p.total_points END
        ) AS geom
    FROM cuts c
    JOIN pts p ON p.int_id = c.int_id
    WHERE
		-- keeps vertices from start to end 
		-- e.g. 0, 1, 2, 3, 4, 5
		(c.start_id < c.end_id AND p.pt_no >= c.start_id AND p.pt_no <= c.end_id)
		-- or wrap around segment where vertices are not in order
		-- e.g. 4, 5, 0, 1, 2, 3
       OR (c.start_id > c.end_id AND (p.pt_no >= c.start_id OR p.pt_no <= c.end_id))
    GROUP BY c.int_id, c.polytype, c.start_id, c.end_id
)
SELECT line_id, int_id, polytype, start_id, end_id, ST_Transform(geom, 4326) AS geom, 
-- ratio for line length and point to point distance
-- use later to determine which line is "curve"
       ST_Length(geom) / NULLIF(ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)), 0) AS ratio,
       geom AS geom_proj
FROM segments;

ALTER TABLE gis.curb_all_edges ADD PRIMARY KEY (line_id);
CREATE INDEX ON gis.curb_all_edges USING gist (geom);
CREATE INDEX ON gis.curb_all_edges USING gist (geom_proj);
CREATE INDEX ON gis.curb_all_edges (polytype);

CREATE TABLE gis.curb_curves AS
SELECT * FROM gis.curb_all_edges
WHERE polytype = 'Intersection' AND ratio > 1.01;

CREATE INDEX ON gis.curb_curves USING gist (geom_proj);
CREATE INDEX ON gis.curb_curves (int_id);


CREATE TABLE gis.curb_shared_edges AS
SELECT i.line_id AS intersection_line_id, r.line_id AS road_line_id
FROM gis.curb_all_edges i
JOIN gis.curb_all_edges r
  ON r.polytype = 'Road Edge' AND ST_Intersects(i.geom, r.geom)
WHERE i.polytype = 'Intersection'
  AND ST_GeometryType(ST_Intersection(i.geom, r.geom)) = 'ST_LineString';
  
CREATE INDEX ON gis.curb_shared_edges (road_line_id);
CREATE INDEX ON gis.curb_shared_edges (intersection_line_id);


CREATE TABLE gis.curb_extensions AS
WITH road_candidates AS (
    SELECT
        c.int_id,
        c.line_id AS curve_line_id,
        r.line_id AS road_line_id,
        r.geom,
        r.geom_proj,
        r.ratio,
		-- see whether the end or the start point of a line is closer to the curve
		-- use to determine which end to extend 
        ST_Distance(ST_StartPoint(r.geom_proj), c.geom_proj)
          < ST_Distance(ST_EndPoint(r.geom_proj), c.geom_proj) AS from_start 
    FROM gis.curb_curves c
    JOIN gis.curb_all_edges r ON ST_DWithin(r.geom_proj, c.geom_proj, 1.5) --road edges within 1.5m of a curve
    WHERE r.polytype = 'Road Edge'
      AND NOT EXISTS ( -- remove the odd dupped lines that are not curb
            SELECT intersection_line_id 
            FROM gis.curb_shared_edges s
            WHERE s.road_line_id = r.line_id
          )
      AND NOT ( -- make sure the start and end of a road candidate does not touch
	  -- the start and end of the curve
	  -- cause we are only extending if EITHER start/end needs to be extended
            ST_DWithin(ST_StartPoint(r.geom_proj), c.geom_proj, 1) AND ST_DWithin(ST_EndPoint(r.geom_proj), c.geom_proj, 1)
          )
), 

-- remove road edges that are really just a straight line 
-- between two intersections aka not a curb
straight_roads AS (
    SELECT int_id, road_line_id
    FROM road_candidates
    WHERE ratio < 2 -- and are kinda straight
    GROUP BY int_id, road_line_id
    HAVING COUNT(1) > 1
),

road_candidates_clean AS (
    SELECT rc.*
    FROM road_candidates rc
    WHERE rc.road_line_id not in ( SELECT road_line_id FROM straight_roads)
),

end_info AS (
    SELECT
        rc.int_id,
        rc.curve_line_id,
        rc.road_line_id,
        rc.geom AS original_geom,
        rc.from_start,
        ST_Length(rc.geom_proj) AS road_length_m,
        CASE WHEN rc.from_start is TRUE THEN ST_StartPoint(rc.geom)
             ELSE ST_EndPoint(rc.geom)
        END AS touch_pt, -- if from_start is true then the start point is the point
						  -- touching the curve that would need to be extended
		-- get ratio of where to start to extend
		-- usually its 10m of the touch point, but if the line is shorter than 10m then 
		-- use the whole line
        CASE WHEN rc.from_start is TRUE
             THEN LEAST(1, 10.0 / ST_Length(rc.geom_proj)) 
             ELSE GREATEST(0, (ST_Length(rc.geom_proj) - 10.0) / ST_Length(rc.geom_proj))
        END AS far_pt_ratio
    FROM road_candidates_clean rc
),

far_points AS (
    SELECT
        e.*,
		-- grab the far point 
        ST_LineInterpolatePoint(e.original_geom, e.far_pt_ratio) AS far_pt
    FROM end_info e
),
extension_lines AS (
    SELECT
        f.int_id,
        f.curve_line_id,
        f.road_line_id,
        f.original_geom,
        f.touch_pt,
		-- extend a 100 m line (which should be long enough
		-- to touch the other extension line
		-- starting at the touching end, pointing the same
		-- way the road is heading
        ST_MakeLine(
            f.touch_pt,
            ST_Project(
                f.touch_pt::geography,100, ST_Azimuth(f.far_pt::geography, f.touch_pt::geography)
            )::geometry
        ) AS extended_line
    FROM far_points f
),
extension_azimuths AS (
    SELECT
        *,
        DEGREES(ST_Azimuth(
            ST_StartPoint(ST_Transform(extended_line, 2952)),
            ST_EndPoint(ST_Transform(extended_line, 2952))
        )) AS az_deg
    FROM extension_lines
),
extension_pairs AS (
    SELECT
        a.road_line_id,
        a.original_geom,
        a.touch_pt,
		-- find where extended line meets 
        ST_Intersection(a.extended_line, b.extended_line) AS meet_pt,
		ABS(a.az_deg - b.az_deg) AS raw_diff_deg
     FROM extension_azimuths a
    JOIN extension_azimuths b ON  a.curve_line_id = b.curve_line_id
      AND a.int_id = b.int_id -- of the same intersection only
    WHERE a.road_line_id != b.road_line_id
)

SELECT DISTINCT ON (road_line_id, touch_pt)
    road_line_id,
    original_geom,
    ST_MakeLine(touch_pt, meet_pt) AS extension,
	LEAST(raw_diff_deg, 360 - raw_diff_deg) AS angle_deg
FROM extension_pairs
 -- make sure the extended lines only touches once
 -- and the intersected point is actually a point 
 -- so no multipoint (intersected a lot of times)
 -- or line (extended lines are in parrarell and intersection is a line)
WHERE GeometryType(meet_pt) = 'POINT'
ORDER BY road_line_id, touch_pt;

CREATE INDEX curb_extensions_geom_2952_idx
    ON gis.curb_extensions USING gist (ST_Transform(extension, 2952));

CREATE TABLE gis.curb_extended AS
-- union the extended portion and the original line
WITH extended_roads AS (
    SELECT
        road_line_id AS line_id,
        ST_LineMerge(ST_Union(geom)) AS geom,
        'extended' AS source
    FROM (
	    SELECT road_line_id, original_geom AS geom
	    FROM gis.curb_extensions
	    UNION ALL
	    SELECT road_line_id, extension AS geom
	    FROM gis.curb_extensions
)road_pieces
    GROUP BY road_line_id
),

-- find road edges that were not extend or attached to a intersection
-- we needed to add back in the layer
-- e.g. a midblock line that is not touching any intersection
untouched_roads AS (
    SELECT
        e.line_id,
        e.geom,
        'untouched road' AS source
    FROM gis.curb_all_edges e
    WHERE e.polytype = 'Road Edge'
      AND line_id NOT IN (SELECT road_line_id FROM gis.curb_extensions)
      AND line_id NOT IN (SELECT road_line_id FROM gis.curb_shared_edges)
),

-- intersection lines that are straight and not shared
-- keep Intersection lines unless they are curved
-- e.g. the straight curb in a three way intersection
straight_intersections AS (
    SELECT
        e.line_id,
        e.geom,
        'straight intersection edge' AS source
    FROM gis.curb_all_edges e
    WHERE e.polytype = 'Intersection'
      AND line_id NOT IN (SELECT line_id FROM gis.curb_curves)
      AND line_id NOT IN (SELECT intersection_line_id FROM gis.curb_shared_edges)
)

-- union them all 
SELECT line_id, geom, source FROM extended_roads
UNION ALL
SELECT line_id, geom, source FROM untouched_roads
UNION ALL
SELECT line_id, geom, source FROM straight_intersections;

CREATE INDEX curb_extended_geom_2952_idx
    ON gis.curb_extended USING gist (ST_Transform(geom, 2952));

-----------------------------------------------------------
-- CLEANING
-----------------------------------------------------------
-- remove odd lines that are resulted from medians
DELETE FROM gis.curb_extended
where line_id  in (
    SELECT line_id
    FROM gis.curb_all_edges
    WHERE polytype = 'Road Edge'
      AND ST_Distance(ST_StartPoint(geom_proj), ST_EndPoint(geom_proj)) < 6   -- start and end point is close
      AND ST_Length(geom_proj) > 5 * ST_Distance(ST_StartPoint(geom_proj), ST_EndPoint(geom_proj))  --but the line is like 5 times longer
);

-- remove the lines they are duplicated
-- essentially the lines that are not curb lines 
DELETE FROM gis.curb_extended
WHERE line_id IN (
    SELECT a.line_id
    FROM gis.curb_extended a
    JOIN gis.curb_extended b
      ON  a.line_id <> b.line_id
      AND a.source <> b.source
      AND ST_DWithin(ST_Transform(a.geom, 2952), ST_Transform(b.geom, 2952), 0.5)
    WHERE a.source <> 'extended'
      AND b.source <> 'extended'
      AND ST_HausdorffDistance(ST_Transform(a.geom, 2952), ST_Transform(b.geom, 2952)) < 0.5 -- simliar in geometry
);


-----------------------------------------------
-- Confidence Level
-----------------------------------------------
ALTER TABLE gis.curb_extended
ADD COLUMN IF NOT EXISTS confidence_level text;


-- Update confidence level 
WITH extension_checks AS (
    SELECT
        ex.road_line_id,
        ex.angle_deg,
        (
         SELECT count(*)
         FROM gis.curb_extended o --figure out overlapping extended lines
         WHERE o.line_id != ex.road_line_id
            AND ST_Intersects(ST_Transform(ex.extension, 2952), ST_Transform(o.geom, 2952)) -- where intersect
              AND ( ST_Crosses(ST_Transform(ex.extension, 2952), ST_Transform(o.geom, 2952)) -- crosses
                   OR ST_Overlaps(ST_Transform(ex.extension, 2952), ST_Transform(o.geom, 2952))) -- OR overlap (like on top of each other)
              AND ST_Distance(
                      ST_StartPoint(ST_Transform(ex.extension, 2952)),
                      ST_Intersection(ST_Transform(ex.extension, 2952), ST_Transform(o.geom, 2952))
                  ) > 1.5 -- except the start and end point that might be touching
        ) AS n_overlaps
    FROM gis.curb_extensions ex
),

extension_confidence AS (
    SELECT
        road_line_id,
        CASE
            WHEN n_overlaps > 0   THEN 0
            WHEN angle_deg >= 75 and angle_deg <= 105 THEN 3 --90degree ish should be pretty good
            WHEN angle_deg >= 45 AND angle_deg <= 135 THEN 2 -- not as 90 degree but should be ok 
            ELSE 1 -- too small of an angle probably?
        END AS confidence_rank
    FROM extension_checks
),

-- a road line extended at both ends has two extensions
-- the road line gets the lower confidence of the two
road_confidence AS (
    SELECT
        road_line_id,
        MIN(confidence_rank) AS confidence_rank
    FROM extension_confidence
    GROUP BY road_line_id
)

UPDATE gis.curb_extended e
SET confidence_level = CASE c.confidence_rank
        WHEN 3 THEN 'high'
        WHEN 2 THEN 'medium'
        WHEN 1 THEN 'low'
        ELSE 'very low' -- the overlapping ones
    END
FROM road_confidence c
WHERE e.line_id = c.road_line_id
  AND e.source = 'extended';