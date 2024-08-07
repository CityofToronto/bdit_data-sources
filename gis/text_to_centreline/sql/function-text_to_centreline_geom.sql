-- FUNCTION: gis.text_to_centreline_geom(text, text, text)

DROP FUNCTION IF EXISTS gis.text_to_centreline_geom (text, text, text);

CREATE OR REPLACE FUNCTION gis.text_to_centreline_geom(
    _street text,
    _from_loc text,
    _to_loc text,
    _return_geom OUT geometry
)
LANGUAGE 'plpgsql'

COST 100
VOLATILE 
AS $BODY$

BEGIN

_return_geom := ST_LINEMERGE(ST_Union(line_geom)) AS geom FROM 
gis.text_to_centreline(0,
                                 _street ,
                                 _from_loc ,
                                 _to_loc);
END;
$BODY$;

ALTER FUNCTION gis.text_to_centreline_geom(text, text, text) OWNER TO gis_admins;

GRANT EXECUTE ON FUNCTION gis.text_to_centreline_geom(text, text, text) TO bdit_humans;
COMMENT ON FUNCTION gis.text_to_centreline_geom(text, text, text) IS
'Wrapper function to the text to centreline functions to return only a single line geometry.
_street is the streetname
_from_loc is the starting point, preferably a street name of an intersection
_from_loc is the ending point, preferably a street name of an intersection';