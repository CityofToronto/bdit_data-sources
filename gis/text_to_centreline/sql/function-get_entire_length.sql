DROP FUNCTION IF EXISTS gwolofs._get_entire_length (text);
CREATE OR REPLACE FUNCTION gwolofs._get_entire_length(highway2_before_editing text)
RETURNS TABLE (
    centreline_id numeric,
    linear_name_full varchar,
    objectid integer,
    geom geometry,
    feature_code int,
    feature_code_desc varchar
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
    centreline_id,
    linear_name_full,
    objectid,
    geom,
    feature_code,
    feature_code_desc
FROM gis_core.centreline_latest
WHERE linear_name_full LIKE highway2;

RAISE NOTICE 'Entire segment found for %', highway2_before_editing;

END;
$BODY$;

COMMENT ON FUNCTION gwolofs._get_entire_length(text) IS '
For bylaws with ''Entire Length'', 
get all the individual line_geom that constitute the whole road segment from gis_core.centreline_latest table.';
