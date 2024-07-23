DROP FUNCTION IF EXISTS gwolofs.bylaws_route_id;

CREATE OR REPLACE FUNCTION gwolofs.bylaws_route_id(
    _bylaw_id integer,
    highway2 text,
    _int_start integer,
    _int_end integer
)
RETURNS TABLE (
    bylaw_id integer,
    int_start integer,
    int_end integer,
    line_geom geometry,
    seq integer,
    geo_id integer,
    lf_name character varying,
    objectid integer,
    fcode integer, fcode_desc character varying
)

LANGUAGE 'plpgsql'

COST 100
VOLATILE
ROWS 1000
AS $BODY$

BEGIN
    
RETURN QUERY    
SELECT
    _bylaw_id,
    rout.int_start,
    rout.int_end,
    rout.geom AS line_geom,
    rout.seq,
    rout.geo_id,
    rout.lf_name,
    rout.objectid,
    rout.fcode,
    rout.fcode_desc
FROM gwolofs._get_lines_btwn_interxn(highway2, _int_start, _int_end) rout;

END;
$BODY$;
