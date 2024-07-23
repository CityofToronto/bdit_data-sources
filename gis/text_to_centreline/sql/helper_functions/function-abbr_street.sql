CREATE OR REPLACE FUNCTION gwolofs.abbr_street(
    _input_street text
)
RETURNS text
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$
DECLARE
    _abbrev_street text;
begin
_abbrev_street := regexp_REPLACE(_input_street,  '[\t\v\b\r\n\u00a0]', ' ');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Cc]ourt)(?![a-z])(?! [A-Z])', 'Crt', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Drive', ' Dr', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Aa]venue)(?![a-z])(?! [A-Z])', 'Ave', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Street', ' St', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Road', ' Rd', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Cc]rescent)(?![a-z])(?! [A-Z])', 'Cres', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Boulevard', ' Blvd', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Tt]errace)(?![a-z])(?! [A-Z])', 'Ter', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Ee]ast)(?![a-z])(?! [A-Z])', 'E', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Ww]est)(?![a-z])(?! [A-Z])', 'W', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Nn]orth)(?![a-z])(?! [A-Z])', 'N', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Ss]outh)(?![a-z])(?! [A-Z])', 'S', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Parkway', ' Pkwy', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Place', ' Pl', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Square', ' Sq', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Circle', ' Crcl', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Trail', ' Trl', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )([Gg]ardens)(?![a-z])(?! [A-Z])', 'Gdns', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, 'metres', 'm', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Avenue E', ' Ave E', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Avenue W', ' Ave W', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Avenue N', ' Ave N', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Avenue S', ' Ave S', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )(Heights)(?![a-z])(?! [A-Z])', 'Hts', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )(Mount)(?![a-z])(?! [A-Z])', 'Mt', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )(Circuit)(?![a-z])(?! [A-Z])', 'Crct', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '(?<=[A-Z][a-z]+ )(Park)(?![a-z])(?! [A-Z])', 'Pk', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, 'Gate', 'Gt', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, 'Pathway', 'Pthwy', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, '\(.*\)', '', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, 'approximately', '', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Grove', ' Grv', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Grv Rd', ' Grove Rd', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, ' Grv Ave', ' Grove Ave', 'g');
_abbrev_street := regexp_REPLACE(_abbrev_street, 'Rd Cres', 'Road Cres', 'g');
return _abbrev_street;
end;
$BODY$;
