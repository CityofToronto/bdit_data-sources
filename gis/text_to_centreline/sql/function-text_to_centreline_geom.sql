-- FUNCTION: gis.text_to_centreline_geom(text, text, text, boolean)

DROP FUNCTION IF EXISTS gis.text_to_centreline_geom (text, text, text, boolean);

CREATE OR REPLACE FUNCTION gis.text_to_centreline_geom(
    _street text,
    _from_loc text,
    _to_loc text,
    _trim boolean
)
RETURNS TABLE (
    _return_geom geometry,
    _return_validation text
)
LANGUAGE 'plpgsql'

COST 100
VOLATILE 
AS $BODY$

DECLARE
    _cleaned_name text;

BEGIN

--- retrieve lf_name for validation
SELECT highway2 
INTO _cleaned_name
FROM gis._clean_bylaws_text(NULL, _street, NULL, NULL);

--- retrieve and treat fields from text_to_centreline

--- trimmed option
IF _trim THEN

	RETURN QUERY
	SELECT
		ST_LINEMERGE(ST_Union(line_geom)),
		format('%s: 100%% (forced)', _cleaned_name)
	FROM gis.text_to_centreline(0,
	                                 _street ,
	                                 _from_loc ,
	                                 _to_loc)
	WHERE lf_name = _cleaned_name;

--- full result
ELSE

	RETURN QUERY
	SELECT
		ST_LINEMERGE(ST_Union(line_geom)),
		format('%s: %s%%', _cleaned_name, round(AVG( ( lf_name = _cleaned_name)::int) * 100, 0))
	FROM gis.text_to_centreline(0,
	                                 _street ,
	                                 _from_loc ,
	                                 _to_loc);

END IF;
END;
$BODY$;

ALTER FUNCTION gis.text_to_centreline_geom(text, text, text, boolean) OWNER TO gis_admins;

GRANT EXECUTE ON FUNCTION gis.text_to_centreline_geom(text, text, text, boolean) TO bdit_humans;
COMMENT ON FUNCTION gis.text_to_centreline_geom(text, text, text, boolean) IS
'Wrapper function to the text to centreline functions to return only a single line geometry.
_street is the streetname
_from_loc is the starting point, preferably a street name of an intersection
_from_loc is the ending point, preferably a street name of an intersection
_trim is a boolean input which determines whether to keep non-matching road segments (detours)';