CREATE OR REPLACE FUNCTION gis.clean_directions(
    txt text
)
RETURNS text
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$
BEGIN
txt := regexp_REPLACE(txt, '( north/west )|( north west )|( west/north )', ' Northwest ', 'gi');
txt := regexp_REPLACE(txt, '( north/east )|( north east )|( east/north )', ' Northeast ', 'gi');
txt := regexp_REPLACE(txt, '( south/east )|( south east )|( east/south )', ' Southeast ', 'gi');
txt := regexp_REPLACE(txt, '( south/west )|( south west )|( west/south )', ' Southwest ', 'gi');
RETURN txt;
END;
$BODY$;

COMMENT ON FUNCTION gis.clean_directions(text)
IS 'Clean directions functions to match the quirks of gis.text_to_centreline.';