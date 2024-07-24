DROP FUNCTION gis._get_entire_length (text);
CREATE OR REPLACE FUNCTION gis._get_entire_length(highway2_before_editing text)
RETURNS TABLE (
    geo_id numeric,
    lf_name varchar,
    objectid numeric,
    line_geom geometry,
    fcode int,
    fcode_desc varchar
)
LANGUAGE 'plpgsql' STRICT STABLE
AS $BODY$

-- i.e. "Ridley Blvd" and "Entire length"

DECLARE

highway2 text :=
    CASE WHEN TRIM(highway2_before_editing) LIKE 'GARDINER EXPRESSWAY%'
    THEN 'F G Gardiner Xy W'
    WHEN highway2_before_editing = 'Don Valley Pky'
    THEN 'Don Valley Parkway'
    ELSE highway2_before_editing
    END;

BEGIN

RETURN QUERY
SELECT centre.geo_id, centre.lf_name, centre.objectid, centre.geom AS line_geom,
centre.fcode, centre.fcode_desc
FROM gis.centreline centre
WHERE centre.lf_name = highway2;

RAISE NOTICE 'Entire segment found for %', highway2_before_editing;

END;
$BODY$;

COMMENT ON FUNCTION gis._get_entire_length(text) IS '
For bylaws with ''Entire Length'', 
get all the individual line_geom that constitute the whole road segment from gis.centreline table.';
