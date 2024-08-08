DROP FUNCTION IF EXISTS gis._get_entire_length (text);
CREATE OR REPLACE FUNCTION gis._get_entire_length(highway2_before_editing text)
RETURNS TABLE (
    centreline_id integer,
    linear_name_full text,
    objectid integer,
    geom geometry,
    feature_code int,
    feature_code_desc text
)
LANGUAGE 'plpgsql' STRICT STABLE
AS $BODY$

-- i.e. "Ridley Blvd" and "Entire length"

DECLARE

highway2 text :=
    CASE
        WHEN TRIM(highway2_before_editing) LIKE 'GARDINER EXPRESSWAY%' THEN 'F G Gardiner Xy %'
        WHEN highway2_before_editing = 'Don Valley Pky' THEN 'Don Valley Parkway %'
        ELSE highway2_before_editing
    END;

BEGIN

RETURN QUERY
SELECT
    cl.centreline_id,
    cl.linear_name_full,
    cl.objectid,
    cl.geom,
    cl.feature_code,
    cl.feature_code_desc
FROM gis_core.centreline_latest AS cl
WHERE cl.linear_name_full LIKE highway2;

RAISE NOTICE 'Entire segment found for %', highway2_before_editing;

END;
$BODY$;

COMMENT ON FUNCTION gis._get_entire_length(text) IS '
For bylaws with ''Entire Length'', 
get all the individual line_geom that constitute the whole road segment from gis_core.centreline_latest table.';
