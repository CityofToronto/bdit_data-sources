-- Create new table while performing separation of tx column to dt (Date) and tod (time)
-- Attach as a partition table to the new parent table
-- then drops the old child table

CREATE OR REPLACE FUNCTION here_staging.move_data(_yyyy text, _mm text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE 
	_startdate DATE;
	_enddate DATE;
	_tablename TEXT;
	_constraintname TEXT;
	
BEGIN
	_startdate:= to_date(_yyyy||'-'||_mm||'-01', 'YYYY-MM-DD');
	_enddate:= _startdate + INTERVAL '1 month';
    _tablename:= 'ta_'||_yyyy||_mm;
	_constraintname:= 'y'||_yyyy||'m'||_mm;

	EXECUTE format($$CREATE TABLE here_staging.%I  AS
						SELECT link_dir, tx::date as dt, tx::time without time zone as tod, tx, length, mean, stddev, min_spd, max_spd, pct_50, pct_85, confidence, null::integer as sample_size
						FROM here.%I ;
					ALTER TABLE here_staging.%I OWNER TO natalie;
				    ALTER TABLE here_staging.%I  ADD CONSTRAINT %I -- Adding constraint so the system can skip the scan to validate the implicit partition constraint. 
                                                                    --Without the CHECK constraint, the table will be scanned to validate the partition constraint 
                                                                   --while holding an ACCESS EXCLUSIVE lock on the parent table. 
        				CHECK (dt>= %L AND dt< %L);
				    ALTER TABLE here_staging.%I ALTER COLUMN link_dir SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN dt SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN tod SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN tx SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN length SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN mean SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN stddev SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN min_spd SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN max_spd SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN pct_50 SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN pct_85 SET NOT NULL ;
					ALTER TABLE here_staging.%I ALTER COLUMN confidence SET NOT NULL ;
					ALTER TABLE here_staging.%I ADD UNIQUE(dt, tod, link_dir);
					ALTER TABLE here_staging.ta ATTACH PARTITION here_staging.%I
        				FOR VALUES FROM (%L) TO (%L);
				    ALTER TABLE here_staging.%I  DROP CONSTRAINT %I; -- drop the redundant CHECK constraint after ATTACH PARTITION is finished.
				    DROP TABLE here.%I;$$,  
				   					_tablename, _tablename, _tablename,_tablename, 
									_constraintname, _startdate, _enddate, 
   									_tablename,_tablename, _tablename, _tablename, 
				   					_tablename, _tablename, _tablename, _tablename, 
				   					_tablename, _tablename, _tablename, _tablename, 
				   					_tablename,_tablename, _startdate, _enddate,
				  					_tablename, _constraintname, _tablename);
END;
$BODY$;