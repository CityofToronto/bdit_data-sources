-- Create staging schema for transitioning 
CREATE SCHEMA here_staging
    AUTHORIZATION natalie;

COMMENT ON SCHEMA here_staging
    IS 'A staging schema for transitioning here.ta table from inheritance partitioning to declarative partitioning. This schema should get dropped once the transition is finished. Created on 2022-08-17.';
    

-- Create parent table in staging schema
CREATE TABLE IF NOT EXISTS here_staging.ta
(
    link_dir text NOT NULL,
	dt date NOT NULL,
	tod time without time zone NOT NULL,
	tx timestamp without time zone NOT NULL, 
    length integer,
    mean numeric(4,1) NOT NULL,
    stddev numeric(4,1) NOT NULL,
    min_spd integer NOT NULL,
    max_spd integer NOT NULL,
    pct_50 integer NOT NULL,
    pct_85 integer NOT NULL,
    confidence integer NOT NULL,
	sample_size integer
) PARTITION BY RANGE (dt);


-- Create view in staging schema
CREATE VIEW here_staging.ta_view AS
 
select link_dir, tx, length, mean, stddev, min_spd, max_spd, pct_50, pct_85, confidence, sample_size 

from here_staging.ta;


-- Create insert function in staging schema
CREATE OR REPLACE FUNCTION here_staging.here_insert_trigger()
    RETURNS trigger 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN 

INSERT INTO here_staging.ta VALUES (NEW.link_dir, 
									NEW.tx::date,
									NEW.tx::time without time zone,
									NEW.tx,
									NEW.length,
									NEW.mean,
									NEW.stddev,
									NEW.min_spd,
									NEW.max_spd,
									NEW.confidence,
									NEW.sample_size); 
RETURN NULL;
END;
$BODY$;

-- Create insert trigger in staging schema
CREATE TRIGGER transform_trigger
        INSTEAD OF INSERT
        ON here_staging.ta_view
        FOR EACH ROW
        EXECUTE PROCEDURE here_staging.here_insert_trigger();

-- Create function to create yearly partition table in staging schema
CREATE OR REPLACE FUNCTION here_staging.create_yearly_tables(yyyy text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE 
	startdate DATE;
	enddate DATE;
	yyyymm TEXT;
	basetablename TEXT := 'ta_';
	tablename TEXT;

BEGIN
    FOR mm IN 01..12 LOOP
        startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
		enddate:= startdate + INTERVAL '1 month';
        IF mm < 10 THEN
            yyyymm:= yyyy||'0'||mm;
        ELSE
            yyyymm:= yyyy||''||mm;
        END IF;
        tablename:= basetablename||yyyymm;
        EXECUTE format($$CREATE TABLE here_staging.%I 
                        PARTITION OF here_staging.ta
                        FOR VALUES FROM  (%L) TO (%L);
                        CREATE INDEX ON here_staging.%I  (link_dir);
                       	CREATE INDEX ON here_staging.%I (tod);
  						CREATE INDEX ON here_staging.%I (dt);
                        $$
                        , tablename, startdate, enddate, tablename, tablename, tablename);
    END LOOP;
END;
$BODY$;