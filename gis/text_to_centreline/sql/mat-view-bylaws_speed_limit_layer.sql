CREATE MATERIALIZED VIEW gis.bylaws_speed_limit_layer
TABLESPACE pg_default
AS
 WITH whole AS (
         SELECT bylaws.id AS bylaw_id,
            centre.lf_name,
            bylaws.geo_id,
            bylaws.speed_limit_km_per_h AS speed_limit,
            bylaws.int1,
            bylaws.int2,
            bylaws.con,
            bylaws.note,
            bylaws.line_geom AS geom,
            bylaws.section,
            bylaws.oid1_geom,
            bylaws.oid1_geom_translated,
            bylaws.oid2_geom,
            bylaws.oid2_geom_translated,
            bylaws.date_added,
            bylaws.date_repealed
           FROM gis.centreline centre
             LEFT JOIN gis.bylaws_routing_dates bylaws USING (geo_id)
          WHERE (bylaws.date_added < now()::date OR bylaws.date_added IS NULL) 
	 		AND (bylaws.date_repealed > now()::date OR bylaws.date_repealed IS NULL)
	 		AND ST_AsText(bylaws.line_geom) != 'GEOMETRYCOLLECTION EMPTY'
        ), whole_added AS (
         SELECT DISTINCT ON (whole.geo_id) whole.bylaw_id,
            whole.lf_name,
            whole.geo_id,
            whole.speed_limit,
            whole.int1,
            whole.int2,
            whole.con,
            whole.note,
            whole.geom,
            whole.section,
            whole.oid1_geom,
            whole.oid1_geom_translated,
            whole.oid2_geom,
            whole.oid2_geom_translated,
            whole.date_added,
            whole.date_repealed
           FROM whole
          WHERE whole.geo_id IS NOT NULL
          ORDER BY whole.geo_id, whole.date_added DESC NULLS LAST, whole.bylaw_id DESC
        ), no_bylaw AS (
         SELECT NULL::integer AS bylaw_id,
            centreline.lf_name,
            centreline.geo_id,
            50 AS speed_limit,
            NULL::integer AS int1,
            NULL::integer AS int2,
            NULL::text AS con,
            NULL::text AS note,
            centreline.geom,
            NULL::numrange AS section,
            NULL::geometry AS oid1_geom,
            NULL::geometry AS oid1_geom_translated,
            NULL::geometry AS oid2_geom,
            NULL::geometry AS oid2_geom_translated,
            NULL::date AS date_added,
            NULL::date AS date_repealed
           FROM gis.centreline
          WHERE NOT (centreline.geo_id IN ( SELECT whole_added.geo_id
                   FROM whole_added)) AND (centreline.fcode_desc::text = ANY (ARRAY['Collector'::character varying, 'Collector Ramp'::character varying, 'Expressway'::character varying, 'Expressway Ramp'::character varying, 'Local'::character varying, 'Major Arterial'::character varying, 'Major Arterial Ramp'::character varying, 'Minor Arterial'::character varying, 'Minor Arterial Ramp'::character varying, 'Pending'::character varying, 'Other'::character varying]::text[]))
        ), part_one AS (
         SELECT whole_added.bylaw_id,
            whole_added.lf_name,
            whole_added.geo_id,
            whole_added.speed_limit,
            whole_added.int1,
            whole_added.int2,
            whole_added.con,
            whole_added.note,
            whole_added.geom,
            whole_added.section,
            whole_added.oid1_geom,
            whole_added.oid1_geom_translated,
            whole_added.oid2_geom,
            whole_added.oid2_geom_translated,
            whole_added.date_added,
            whole_added.date_repealed
           FROM whole_added
          WHERE whole_added.section IS NOT NULL AND whole_added.section <> '[0,1]'::numrange
        ), next_bylaw AS (
         SELECT b.id AS bylaw_id,
            b.lf_name,
            b.geo_id,
            b.speed_limit_km_per_h AS speed_limit,
            b.int1,
            b.int2,
            b.con,
            b.note,
            b.line_geom AS geom,
                CASE
                    WHEN b.section IS NULL THEN '[0,1]'::numrange
                    ELSE b.section
                END AS section,
            b.oid1_geom,
            b.oid1_geom_translated,
            b.oid2_geom,
            b.oid2_geom_translated,
            b.date_added,
            b.date_repealed
           FROM part_one one
             JOIN LATERAL ( SELECT bylaws.id,
                    bylaws.lf_name,
                    bylaws.geo_id,
                    bylaws.speed_limit_km_per_h,
                    bylaws.int1,
                    bylaws.int2,
                    bylaws.con,
                    bylaws.note,
                    bylaws.line_geom,
                    bylaws.section,
                    bylaws.oid1_geom,
                    bylaws.oid1_geom_translated,
                    bylaws.oid2_geom,
                    bylaws.oid2_geom_translated,
                    bylaws.date_added,
                    bylaws.date_repealed
                   FROM gis.bylaws_routing_dates bylaws
                  WHERE bylaws.geo_id = one.geo_id AND (bylaws.date_added < one.date_added OR bylaws.id < one.bylaw_id)
                  ORDER BY bylaws.date_added DESC NULLS LAST, one.bylaw_id DESC
                 LIMIT 1) b ON b.geo_id = one.geo_id
        ), part_two AS (
         SELECT next_bylaw.bylaw_id,
            next_bylaw.lf_name,
            next_bylaw.geo_id,
            next_bylaw.speed_limit,
            next_bylaw.int1,
            next_bylaw.int2,
            next_bylaw.con,
            next_bylaw.note,
            st_difference(next_bylaw.geom, st_buffer(part_one.geom, 0.00001::double precision)) AS geom,
            next_bylaw.section - part_one.section AS section,
            next_bylaw.oid1_geom,
            next_bylaw.oid1_geom_translated,
            next_bylaw.oid2_geom,
            next_bylaw.oid2_geom_translated,
            next_bylaw.date_added,
            next_bylaw.date_repealed
           FROM part_one
             JOIN next_bylaw USING (geo_id, lf_name)
          WHERE (next_bylaw.section - part_one.section) <> 'empty'::numrange
        ), part_one_without_bylaw AS (
         SELECT NULL::integer AS bylaw_id,
            cl.lf_name,
            part_one.geo_id,
            50 AS speed_limit,
            NULL::integer AS int1,
            NULL::integer AS int2,
            NULL::text AS con,
            NULL::text AS note,
            st_difference(cl.geom, st_buffer(part_one.geom, 0.00001::double precision)) AS geom,
            '[0,1]'::numrange - part_one.section AS section,
            NULL::geometry AS oid1_geom,
            NULL::geometry AS oid1_geom_translated,
            NULL::geometry AS oid2_geom,
            NULL::geometry AS oid2_geom_translated,
            NULL::date AS date_added,
            NULL::date AS date_repealed
           FROM part_one
             JOIN gis.centreline cl USING (geo_id)
          WHERE NOT (part_one.geo_id IN ( SELECT part_two.geo_id
                   FROM part_two))
          GROUP BY cl.lf_name, part_one.geo_id, part_one.section, part_one.date_added, cl.geom, part_one.geom
          ORDER BY part_one.geo_id
        ), part_two_without_bylaw AS (
         SELECT NULL::integer AS bylaw_id,
            part_two.lf_name,
            part_one.geo_id,
            50 AS speed_limit,
            NULL::integer AS int1,
            NULL::integer AS int2,
            NULL::text AS con,
            NULL::text AS note,
            st_difference(cl.geom, st_buffer(st_union(part_one.geom, part_two.geom), 0.00001::double precision)) AS geom,
            '[0,1]'::numrange - range_merge(part_one.section, part_two.section) AS section,
            NULL::geometry AS oid1_geom,
            NULL::geometry AS oid1_geom_translated,
            NULL::geometry AS oid2_geom,
            NULL::geometry AS oid2_geom_translated,
            NULL::date AS date_added,
            NULL::date AS date_repealed
           FROM part_one
             JOIN part_two USING (geo_id, lf_name)
             LEFT JOIN gis.centreline cl USING (geo_id, lf_name)
          WHERE (part_one.geo_id IN ( SELECT part_two_1.geo_id
                   FROM part_two part_two_1)) AND range_merge(part_one.section, part_two.section) <> '[0,1]'::numrange
        )
 SELECT no_bylaw.bylaw_id,
    no_bylaw.lf_name,
    no_bylaw.geo_id,
    no_bylaw.speed_limit,
    no_bylaw.int1,
    no_bylaw.int2,
    no_bylaw.con,
    no_bylaw.note,
    no_bylaw.geom,
    no_bylaw.section,
    no_bylaw.oid1_geom,
    no_bylaw.oid1_geom_translated,
    no_bylaw.oid2_geom,
    no_bylaw.oid2_geom_translated,
    no_bylaw.date_added,
    no_bylaw.date_repealed
   FROM no_bylaw
UNION
 SELECT whole_added.bylaw_id,
    whole_added.lf_name,
    whole_added.geo_id,
    whole_added.speed_limit,
    whole_added.int1,
    whole_added.int2,
    whole_added.con,
    whole_added.note,
    CASE WHEN whole_added.section IS NOT NULL 
	THEN st_linesubstring(cl.geom, lower(whole_added.section)::double precision, upper(whole_added.section)::double precision)
	ELSE cl.geom
	END AS geom,
    whole_added.section,
    whole_added.oid1_geom,
    whole_added.oid1_geom_translated,
    whole_added.oid2_geom,
    whole_added.oid2_geom_translated,
    whole_added.date_added,
    whole_added.date_repealed
   FROM whole_added
   JOIN gis.centreline cl USING (geo_id, lf_name)
UNION
 SELECT part_one_without_bylaw.bylaw_id,
    part_one_without_bylaw.lf_name,
    part_one_without_bylaw.geo_id,
    part_one_without_bylaw.speed_limit,
    part_one_without_bylaw.int1,
    part_one_without_bylaw.int2,
    part_one_without_bylaw.con,
    part_one_without_bylaw.note,
    CASE WHEN part_one_without_bylaw.section IS NOT NULL 
	THEN st_linesubstring(cl.geom, lower(part_one_without_bylaw.section)::double precision, upper(part_one_without_bylaw.section)::double precision)
	ELSE cl.geom
	END AS geom,
    part_one_without_bylaw.section,
    part_one_without_bylaw.oid1_geom,
    part_one_without_bylaw.oid1_geom_translated,
    part_one_without_bylaw.oid2_geom,
    part_one_without_bylaw.oid2_geom_translated,
    part_one_without_bylaw.date_added,
    part_one_without_bylaw.date_repealed
   FROM part_one_without_bylaw
   JOIN gis.centreline cl USING (geo_id, lf_name)
UNION
 SELECT part_two.bylaw_id,
    part_two.lf_name,
    part_two.geo_id,
    part_two.speed_limit,
    part_two.int1,
    part_two.int2,
    part_two.con,
    part_two.note,
    CASE WHEN part_two.section IS NOT NULL 
	THEN st_linesubstring(cl.geom, lower(part_two.section)::double precision, upper(part_two.section)::double precision)
	ELSE cl.geom
	END AS geom,
    part_two.section,
    part_two.oid1_geom,
    part_two.oid1_geom_translated,
    part_two.oid2_geom,
    part_two.oid2_geom_translated,
    part_two.date_added,
    part_two.date_repealed
   FROM part_two
   JOIN gis.centreline cl USING (geo_id, lf_name)
UNION
 SELECT part_two_without_bylaw.bylaw_id,
    part_two_without_bylaw.lf_name,
    part_two_without_bylaw.geo_id,
    part_two_without_bylaw.speed_limit,
    part_two_without_bylaw.int1,
    part_two_without_bylaw.int2,
    part_two_without_bylaw.con,
    part_two_without_bylaw.note,
    CASE WHEN part_two_without_bylaw.section IS NOT NULL 
	THEN st_linesubstring(cl.geom, lower(part_two_without_bylaw.section)::double precision, upper(part_two_without_bylaw.section)::double precision)
	ELSE cl.geom
	END AS geom,
    part_two_without_bylaw.section,
    part_two_without_bylaw.oid1_geom,
    part_two_without_bylaw.oid1_geom_translated,
    part_two_without_bylaw.oid2_geom,
    part_two_without_bylaw.oid2_geom_translated,
    part_two_without_bylaw.date_added,
    part_two_without_bylaw.date_repealed
   FROM part_two_without_bylaw
   JOIN gis.centreline cl USING (geo_id, lf_name)
WITH DATA;

COMMENT ON MATERIALIZED VIEW gis.bylaws_speed_limit_layer
    IS 'This view consists of a few parts.

1. no_bylaw -> centrelines not involved in bylaws
2. whole_added -> centrelines involved in bylaws, be it fully or partially
3. part_one_without_bylaw -> parts of centrelines not involved in bylaws if there isnt a next applicable bylaw
4. part_two -> for the partial centrelines, include the next bylaw that applies to it if exists
5. part_two_without_bylaw -> for partial centrelines where the next_bylaw has been applied to it, the remaining part of the centreline not involved in the bylaws

Updated on 2020-08-18.
';
