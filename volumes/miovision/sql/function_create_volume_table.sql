DROP FUNCTION  IF EXISTS miovision_api.create_volume_table(text);

CREATE OR REPLACE FUNCTION miovision_api.create_volume_table(
	_yyyy text)
    RETURNS VOID
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE STRICT 
    SECURITY DEFINER
AS $BODY$

DECLARE
	_startdate DATE;
 	_tablename TEXT;

BEGIN
_startdate:= to_date(_yyyy||'-01-01', 'YYYY-MM-DD');
_tablename:= 'volumes'||_yyyy;

EXECUTE format($$CREATE TABLE miovision_api.%I 
				(CHECK (datetime_bin >= DATE %L AND datetime_bin < DATE %L + INTERVAL '1 year')
        ,FOREIGN KEY (volume_15min_mvt_uid) REFERENCES miovision_api.volumes_15min_mvt (volume_15min_mvt_uid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE SET NULL
				 , UNIQUE(intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
				) INHERITS (miovision_api.volumes)$$
				, _tablename, _startdate, _startdate, _tablename);
			EXECUTE format($$ALTER TABLE miovision_api.%I OWNER TO miovision_admins$$, _tablename);
      EXECUTE format($$ CREATE INDEX ON miovision_api.%I USING brin(datetime_bin) $$, _tablename);
      EXECUTE format($$ CREATE INDEX ON miovision_api.%I (volume_15min_mvt_uid) $$, _tablename);
      EXECUTE format($$ CREATE INDEX ON miovision_api.%I (intersection_uid) $$, _tablename);
END;
$BODY$;

GRANT EXECUTE ON FUNCTION miovision_api.create_volume_table(text) TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.create_volume_table(TEXT) TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.create_volume_table(text) IS 'Create a child volume table for the given year';