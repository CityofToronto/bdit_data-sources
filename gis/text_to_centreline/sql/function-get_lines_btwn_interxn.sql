--USING centrelines aka GIS network
DROP FUNCTION IF EXISTS gis._get_lines_btwn_interxn;

CREATE OR REPLACE FUNCTION gis._get_lines_btwn_interxn(
    _highway2 text,
    _int_start integer,
    _int_end integer
)
RETURNS TABLE (
    int_start integer,
    int_end integer,
    seq integer,
    geo_id integer,
    lf_name text,
    objectid integer,
    geom geometry,
    fcode integer,
    fcode_desc text
)
LANGUAGE 'plpgsql'

COST 100
STABLE STRICT
ROWS 1000
AS $BODY$

BEGIN
RETURN QUERY

WITH results AS (
    SELECT
        _get_lines_btwn_interxn._int_start,
        _get_lines_btwn_interxn._int_end,
        dijkstra.seq,
        dijkstra.edge
    FROM pgr_dijkstra(format('SELECT
                centreline_id AS id,
                from_intersection_id AS source,
                to_intersection_id AS target,
                st_length(st_transform(geom, 2952)) * CASE WHEN levenshtein(TRIM(linear_name_full), TRIM(%L), 1, 1, 1) < 3 THEN 0.3::float ELSE 1.0::float END AS cost
            FROM gis_core.centreline_latest
            WHERE feature_code_desc IN (''Collector'', ''Expressway Ramp'', ''Pending'', ''Expressway'', ''Major Arterial'', ''Local'', ''Minor Arterial'')
            '::text, _highway2),
        _int_start::bigint, _int_end::bigint, FALSE
    ) AS dijkstra
--or do pgr_dijkstra('SELECT id, source::int, target::int, 
    --CASE lf_name WHEN '''|| _highway2 ||''' THEN (0.3*cost)::float ELSE cost END AS cost from gis.centreline_routing_undirected_lfname'::text, ... )
)
SELECT
    results._int_start,
    results._int_end,
    results.seq,
    centre.centreline_id AS geo_id,
    centre.linear_name_full AS lf_name,
    centre.objectid,
    centre.geom,
    centre.feature_code AS fcode,
    centre.feature_code_desc AS fcode_desc 
FROM results
JOIN gis_core.centreline_latest AS centre ON results.edge = centre.centreline_id
--WHERE levenshtein(TRIM(centre.lf_name), TRIM(_highway2), 1, 1, 1) < 3
--instead of `WHERE centre.lf_name = _highway2` because the street name might not be EXACTLY the same 
--(or we can get the output of get_intersection_id for lf_name and input it here but lf_name is not an output for that function currently)
ORDER BY
    results._int_start,
    results._int_end,
    results.seq;

RAISE NOTICE 'pg_routing done for int_start: % and int_end: %', 
_int_start, _int_end;

END;
$BODY$;
