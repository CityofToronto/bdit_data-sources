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
txt := regexp_REPLACE(txt, ' Point ', ' point ');
txt := regexp_REPLACE(txt, ' Between ', ' between ');
txt := regexp_REPLACE(txt, ' Thereof ', ' thereof ');
txt := regexp_REPLACE(txt, ' The ', ' the ');
RETURN txt;
END;
$BODY$;