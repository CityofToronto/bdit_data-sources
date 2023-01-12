DROP FUNCTION  IF EXISTS wys.create_raw_data_table(text);

CREATE OR REPLACE FUNCTION wys.create_raw_data_table(
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
_tablename:= 'raw_data_'||_yyyy;

EXECUTE format($$CREATE TABLE wys.%I (
                PRIMARY KEY (raw_data_uid),
				CHECK (datetime_bin >= DATE %L AND datetime_bin < DATE %L + INTERVAL '1 year')
            	 , UNIQUE(api_id, datetime_bin, speed)
				) INHERITS (wys.raw_data)$$
				, _tablename, _startdate, _startdate, _tablename);

EXECUTE format($$GRANT SELECT, UPDATE, INSERT ON TABLE wys.%I TO wys_bot$$,
                 _tablename);
EXECUTE format($$ CREATE INDEX ON wys.%I USING brin(datetime_bin) $$,        
               _tablename);
EXECUTE format($$ ANALYZE wys.%I $$,        
               _tablename);

END;
$BODY$;

GRANT EXECUTE ON FUNCTION wys.create_raw_data_table(text) TO wys_bot;

COMMENT ON FUNCTION wys.create_raw_data_table(text) IS 'Create a child raw_data table for the given year';