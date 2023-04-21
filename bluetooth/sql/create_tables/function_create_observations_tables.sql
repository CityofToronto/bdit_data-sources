DROP FUNCTION IF EXISTS bluetooth.create_obs_tables(text);

CREATE OR REPLACE FUNCTION bluetooth.create_obs_tables(
    _yyyy text
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE STRICT
SECURITY DEFINER
AS $BODY$

DECLARE
	_startdate DATE;
	_yyyymm TEXT;
	_basetablename TEXT := 'observations_';
	_tablename TEXT;
BEGIN

	FOR _mm IN 01..12 LOOP
		_startdate:= to_date(_yyyy||'-'||_mm||'-01', 'YYYY-MM-DD');
		IF _mm < 10 THEN
			_yyyymm:= _yyyy||'0'||_mm;
		ELSE
			_yyyymm:= _yyyy||''||_mm;
		END IF;
		_tablename:= _basetablename||_yyyymm;
		EXECUTE format($$CREATE TABLE bluetooth.%I 
			(PRIMARY KEY (id),
			CHECK (measured_timestamp > DATE %L AND measured_timestamp <= DATE %L + INTERVAL '1 month'),
			UNIQUE(user_id, analysis_id, measured_time, measured_timestamp)
			) INHERITS (bluetooth.observations);
			ALTER TABLE bluetooth.%I
  				OWNER TO bt_admins;
			CREATE INDEX ON bluetooth.%I (analysis_id);
			CREATE INDEX ON bluetooth.%I (cod);
			CREATE INDEX ON bluetooth.%I USING brin(measured_timestamp);
			$$
			, _tablename, _startdate, _startdate, _tablename, _tablename, _tablename, _tablename);
		
	END LOOP;
END;
$BODY$;

GRANT EXECUTE ON FUNCTION bluetooth.create_obs_tables (text) TO bt_admins;
GRANT EXECUTE ON FUNCTION bluetooth.create_obs_tables (text) TO bt_bot;

COMMENT ON FUNCTION bluetooth.create_obs_tables(
    text
) IS 'Loops through the months of the provided year to create a partitioned table for each month';