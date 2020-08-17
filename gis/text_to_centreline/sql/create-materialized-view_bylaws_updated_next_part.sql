CREATE MATERIALIZED VIEW jchew.bylaws_updated_next_part_filtered AS
--gis.centreline has 67769 rows (only 46584 of them in the fcode_desc we want, 21185 are )
WITH whole AS(
--69487 rows (69303 rows if only include entire centreline)
SELECT bylaws.id AS bylaw_id, centre.lf_name, bylaws.geo_id, bylaws.speed_limit_km_per_h AS speed_limit, 
bylaws.int1, bylaws.int2, con, note, line_geom AS geom, section, 
oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, 
bylaws.date_added, bylaws.date_repealed
FROM gis.centreline centre
LEFT OUTER JOIN jchew.bylaws_routing4_dates bylaws USING (geo_id)
WHERE (date_added < now()::date OR date_added IS NULL)  --Bylaw is in effect
AND (date_repealed > now()::date OR date_repealed IS NULL ) --Bylaw not repealed yet
--AND (section IS NULL OR section = '[0,1]')
)

, whole_added AS (
--21205 rows
SELECT DISTINCT ON (geo_id) bylaw_id, lf_name, geo_id, speed_limit, 
int1, int2, con, note, geom, section, 
oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, 
date_added, date_repealed
FROM whole
WHERE geo_id IS NOT NULL
ORDER BY geo_id, date_added DESC NULLS LAST, bylaw_id DESC
)

, no_bylaw AS (
--25379 rows
SELECT 
NULL::integer AS bylaw_id, lf_name, geo_id, 50 AS speed_limit, 
NULL::integer AS int1, NULL::integer AS int2, NULL::text AS con, NULL::text AS note, 
geom, NULL::numrange AS section, 
NULL::geometry AS oid1_geom, NULL::geometry AS oid1_geom_translated, 
NULL::geometry AS oid2_geom, NULL::geometry AS oid2_geom_translated, 
NULL::date AS date_added, NULL::date AS date_repealed
FROM gis.centreline
WHERE geo_id NOT IN (SELECT geo_id FROM whole_added)
AND fcode_desc IN ('Collector','Collector Ramp','Expressway','Expressway Ramp',
'Local','Major Arterial','Major Arterial Ramp','Minor Arterial',
'Minor Arterial Ramp','Pending', 'Other')
)

, part_one AS (
--151 rows
SELECT * FROM whole_added
WHERE section IS NOT NULL AND section != '[0,1]'
)	

, next_bylaw AS (
SELECT b.id AS bylaw_id, b.lf_name, b.geo_id, b.speed_limit_km_per_h AS speed_limit, b.int1, b.int2, b.con, b.note, 
b.line_geom AS geom, CASE WHEN b.section IS NULL THEN '[0,1]' ELSE b.section END AS section, 
b.oid1_geom, b.oid1_geom_translated, b.oid2_geom, b.oid2_geom_translated, 
b.date_added, b.date_repealed 
FROM part_one one
INNER JOIN LATERAL 
(SELECT id, lf_name, geo_id,
 speed_limit_km_per_h, int1, int2, con, note, line_geom, section,
 oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, 
 date_added, date_repealed
	FROM jchew.bylaws_routing4_dates bylaws
	WHERE bylaws.geo_id = one.geo_id
	AND (bylaws.date_added < one.date_added
		 OR bylaws.id < one.bylaw_id)
	ORDER BY date_added DESC NULLS LAST, bylaw_id DESC
	LIMIT 1) b 
	ON b.geo_id = one.geo_id
)		

, part_two AS (
--18 rows + 2 empty section ones
SELECT next_bylaw.bylaw_id, next_bylaw.lf_name, next_bylaw.geo_id, 
next_bylaw.speed_limit, next_bylaw.int1, next_bylaw.int2, next_bylaw.con, next_bylaw.note, 
ST_Difference(next_bylaw.geom, ST_Buffer(part_one.geom, 0.00001)) AS geom, 
(next_bylaw.section - part_one.section) AS section, 
next_bylaw.oid1_geom, next_bylaw.oid1_geom_translated, next_bylaw.oid2_geom, next_bylaw.oid2_geom_translated, 
next_bylaw.date_added, next_bylaw.date_repealed
FROM part_one
INNER JOIN next_bylaw
USING (geo_id, lf_name)
WHERE (next_bylaw.section - part_one.section) != 'empty' 
--some next_bylaw applied on the same section as the first_bylaw
)
, part_one_without_bylaw AS (
-- 133 rows
SELECT NULL::integer AS bylaw_id, cl.lf_name, geo_id, 50 AS speed_limit, 
NULL::integer AS int1, NULL::integer AS int2, NULL::text AS con, NULL::text AS note, 
ST_Difference(cl.geom, ST_Buffer(part_one.geom, 0.00001)) AS geom, ('[0,1]'::numrange - part_one.section) AS section, 
NULL::geometry AS oid1_geom, NULL::geometry AS oid1_geom_translated, 
NULL::geometry AS oid2_geom, NULL::geometry AS oid2_geom_translated, 
NULL::date AS date_added, NULL::date AS date_repealed
	FROM part_one 
    INNER JOIN gis.centreline cl USING (geo_id)
	WHERE geo_id NOT IN (SELECT geo_id FROM part_two)
	GROUP BY cl.lf_name, geo_id, section, part_one.date_added, cl.geom, part_one.geom
	ORDER BY geo_id
)
, part_two_without_bylaw AS (
--only 1 row
SELECT NULL::integer AS bylaw_id, lf_name, geo_id, 50 AS speed_limit, 
NULL::integer AS int1, NULL::integer AS int2, NULL::text AS con, NULL::text AS note, 
ST_Difference (cl.geom, ST_Buffer(ST_Union(part_one.geom, part_two.geom), 0.00001) ) AS geom,
('[0,1]'::numrange - range_merge(part_one.section, part_two.section))::numrange AS section, 
NULL::geometry AS oid1_geom, NULL::geometry AS oid1_geom_translated, 
NULL::geometry AS oid2_geom, NULL::geometry AS oid2_geom_translated, 
NULL::date AS date_added, NULL::date AS date_repealed
FROM part_one
INNER JOIN part_two USING (geo_id, lf_name)
LEFT JOIN gis.centreline cl USING (geo_id, lf_name)
WHERE part_one.geo_id IN (SELECT geo_id FROM part_two)
AND range_merge(part_one.section, part_two.section) != '[0,1]'
)

SELECT * FROM no_bylaw
UNION
SELECT * FROM whole_added --included partial ones aka part_one
UNION
SELECT * FROM part_one_without_bylaw
UNION
SELECT * FROM part_two --to include next_bylaw for partial ones
UNION
SELECT * FROM part_two_without_bylaw
;

COMMENT ON MATERIALIZED VIEW jchew.bylaws_updated_next_part_filtered
    IS 'This view consists of a few parts.

1. no_bylaw -> centrelines not involved in bylaws
2. whole_added -> centrelines involved in bylaws, be it fully or partially
3. part_one_without_bylaw -> parts of centrelines not involved in bylaws if there isnt a next applicable bylaw
4. part_two -> for the partial centrelines, include the next bylaw that applies to it if exists
5. part_two_without_bylaw -> for partial centrelines where the next_bylaw has been applied to it, the remaining part of the centreline not involved in the bylaws';
