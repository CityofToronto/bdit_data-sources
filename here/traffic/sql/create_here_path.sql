-- Parent table structure
CREATE TABLE IF NOT EXISTS here.ta_path
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
	sample_size integer,
    harmean numeric(4,1) NOT NULL,
    vdt integer NOT NULL,
    probe_count integer NOT NULL
) PARTITION BY RANGE (dt);

ALTER TABLE here.ta_path OWNER TO here_admins;
GRANT SELECT ON TABLE here.ta_path TO bdit_humans;
GRANT INSERT, UPDATE, SELECT, TRIGGER ON TABLE here.ta_path TO here_bot;

-- Simple view structure 
CREATE VIEW here.ta_path_view AS
 
SELECT  link_dir, 
        tx, 
        NULL:integer as epoch_min, 
        length, 
        sample_size, 
        mean, 
        harmonic_mean,
        vdt,
        probe_count,
        stddev, 
        min_spd, 
        max_spd, 
        confidence, 
        pct_50, 
        pct_85 

FROM here.ta_path

ALTER TABLE here.ta_path_view OWNER TO here_admins;
GRANT INSERT, UPDATE, SELECT, TRIGGER ON TABLE here.ta_path_view TO here_bot;
  
CREATE OR REPLACE FUNCTION here.here_insert_trigger_path()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN 

INSERT INTO here.ta_path VALUES (NEW.link_dir, 
									NEW.tx::date,
									NEW.tx::time without time zone,
									NEW.tx,
									NEW.length,
									NEW.mean,
									NEW.stddev,
									NEW.min_spd,
									NEW.max_spd,
									NEW.pct_50, 
									NEW.pct_85,
									NEW.confidence,
									NEW.sample_size,
                                    NEW.harmean,
                                    NEW.vdt,
                                    NEW.probe_count); 
RETURN NULL;
END;
$BODY$;

ALTER FUNCTION here.here_insert_trigger_path()
    OWNER TO here_admins;
  
-- Instead of inserting to ta_view, 
-- insert to parent table
CREATE TRIGGER transform_trigger
        INSTEAD OF INSERT
        ON here.ta_path_view
        FOR EACH ROW
        EXECUTE PROCEDURE here.here_insert_trigger_path();


