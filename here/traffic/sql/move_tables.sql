-- Create new table while performing separation of tx column to dt (Date) and tod (time)
-- Attach as a partition table to the new parent table
-- then drops the old child table

CREATE OR REPLACE FUNCTION here.move_data(_yyyy text, _mm text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE 
	_startdate DATE;
	_enddate DATE;
	_oldtablename TEXT;
	_newtablename TEXT;
	_constraintname TEXT;
	
BEGIN
	_startdate:= to_date(_yyyy||'-'||_mm||'-01', 'YYYY-MM-DD');
	_enddate:= _startdate + INTERVAL '1 month';
    _oldtablename:= 'ta_'||_yyyy||_mm;
	_newtablename:= 'ta_'||_yyyy||_mm||'_new';
	_constraintname:= 'y'||_yyyy||'m'||_mm;

	EXECUTE format($$CREATE TABLE here.%I  AS
						SELECT link_dir, tx::date as dt, tx::time without time zone as tod, tx, length, mean, stddev, min_spd, max_spd, pct_50, pct_85, confidence, null::integer as sample_size
						FROM here.%I ;
					ALTER TABLE here.%I OWNER TO natalie;
				    ALTER TABLE here.%I  ADD CONSTRAINT %I
        				CHECK (dt>= %L AND dt< %L);
				    ALTER TABLE here.%I ALTER COLUMN link_dir SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN dt SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN tod SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN tx SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN length SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN mean SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN stddev SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN min_spd SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN max_spd SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN pct_50 SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN pct_85 SET NOT NULL ;
					ALTER TABLE here.%I ALTER COLUMN confidence SET NOT NULL ;
					ALTER TABLE here.%I ADD UNIQUE(dt, tod, link_dir);
					ALTER TABLE here.ta_new ATTACH PARTITION here.%I  
        				FOR VALUES FROM (%L) TO (%L);
    				ALTER TABLE here.%I DROP CONSTRAINT %I; 
					DROP TABLE here.%I;$$,  _newtablename, _oldtablename, 
											   _newtablename,
				   							   _newtablename, 
											   _constraintname, 
											   _startdate, _enddate, 
                                                _newtablename,_newtablename, _newtablename, _newtablename, _newtablename, _newtablename,
                                              _newtablename, _newtablename, _newtablename, _newtablename, _newtablename, _newtablename, _newtablename, 
                                              _newtablename, 
				  							   _startdate, 
											   _enddate,
				   								_newtablename,
											   _constraintname, 
											   _oldtablename);
END;
$BODY$;
