CREATE OR REPLACE FUNCTION rbahreh.abbr_streetype(
	_input_street text)
    RETURNS text
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
    _abbrev_street TEXT;
begin
_abbrev_street := regexp_REPLACE(_input_street,  '[\t\v\b\r\n\u00a0]', ' ');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Drive', 'Dr');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Street', 'St');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Parkway', 'Pkwy');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Place', 'Pl');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Square', 'Sq');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Circle', 'Crcl');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Trail$', 'Trl');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Gate$', 'Gt');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Gte$', 'Gt');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Pathway', 'Pthwy');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Grove', 'Grv');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Av$', 'Ave');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Aev$', 'Ave');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Ae$', 'Ave');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Aven W', 'Ave W');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Aven$', 'Ave');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Aave$', 'Ave');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Avwe$', 'Ave');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Avve$', 'Ave');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Bd$', 'Blvd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Bl$', 'Blvd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Bld$', 'Blvd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Blve$', 'Blvd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Blbd$', 'Blvd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Boul$', 'Blvd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Bv$', 'Blvd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Bd$', 'Brdg');--Bridge
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Brid$', 'Brdg');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Bdge$', 'Brdg');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Cour$', 'Crt');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Gard$', 'Gdns');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Grov$', 'Grv');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Grve$', 'Grv');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Dri$', 'Dr');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Driv$', 'Dr');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Expr$', 'Exp');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Ex$', 'Exp');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Xy$', 'Exp');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Tr$', 'Trl');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Trai$', 'Trl');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Tril$', 'Trl');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Tral$', 'Trl');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Terr$', 'Ter');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Stre$', 'St');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Str$', 'St');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Squa$', 'Sq');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Rod$', 'Rd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Rr$', 'Rd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Road$', 'Rd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Rd.$', 'Rd');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Path$', 'Pthwy');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Ptwy$', 'Pthwy');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Pk$', 'Pkwy');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Pky$', 'Pkwy');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Park$', 'Pk');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Plac$', 'Pl');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Lne$', 'Ln');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Line$', 'Ln');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Heights$', 'Hts');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Heigh$', 'Hts');
_abbrev_street := regexp_REPLACE(_abbrev_street, '^Heig$', 'Hts');
return _abbrev_street;
end;
$BODY$;