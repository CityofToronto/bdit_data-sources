CREATE OR REPLACE FUNCTION gwolofs.custom_case(
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
txt := regexp_REPLACE(txt, 'From ', 'from ');
txt := regexp_REPLACE(txt, 'To ', 'to ');
txt := regexp_REPLACE(txt, ' A ', ' a '); --not at the beginning, ie. "A point west of"
txt := regexp_REPLACE(txt, 'And ', 'and ');
txt := regexp_REPLACE(txt, 'Of ', 'of ');
txt := regexp_REPLACE(txt, ' M ', ' m ');
RETURN txt;
END;
$BODY$;

COMMENT ON FUNCTION gwolofs.custom_case(txt)
IS 'Transform text input to proper capitalization: initcap for street names +
lower case for separators.';