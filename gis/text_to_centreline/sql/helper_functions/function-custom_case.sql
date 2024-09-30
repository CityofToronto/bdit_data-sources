CREATE OR REPLACE FUNCTION gis.custom_case(
    txt text
)
RETURNS text
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$
DECLARE
    txt text := initcap(custom_case.txt);
BEGIN
txt := regexp_REPLACE(txt, 'From ', 'from ', 'g');
txt := regexp_REPLACE(txt, 'To ', 'to ', 'g');
txt := regexp_REPLACE(txt, ' A ', ' a ', 'g'); --not at the beginning, ie. "A point west of"
txt := regexp_REPLACE(txt, 'And ', 'and ', 'g');
txt := regexp_REPLACE(txt, 'Of ', 'of ', 'g');
txt := regexp_REPLACE(txt, ' Metres ', ' m ', 'g');
txt := regexp_REPLACE(txt, ' M ', ' m ', 'g');
txt := regexp_REPLACE(txt, ' Point ', ' point ', 'g');
txt := regexp_REPLACE(txt, ' Between ', ' between ', 'g');
txt := regexp_REPLACE(txt, ' Thereof ', ' thereof ', 'g');
txt := regexp_REPLACE(txt, ' The ', ' the ', 'g');
--clean directions
txt := regexp_REPLACE(txt, '( north/west )|( north west )|( west/north )', ' Northwest ', 'gi');
txt := regexp_REPLACE(txt, '( north/east )|( north east )|( east/north )', ' Northeast ', 'gi');
txt := regexp_REPLACE(txt, '( south/east )|( south east )|( east/south )', ' Southeast ', 'gi');
txt := regexp_REPLACE(txt, '( south/west )|( south west )|( west/south )', ' Southwest ', 'gi');
RETURN txt;
END;
$BODY$;

COMMENT ON FUNCTION gis.custom_case(text)
IS 'Initcap plus some select lowercasing to fit the quirks of gis.text_to_centreline.';