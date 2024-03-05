-- Parent table structure
CREATE TABLE IF NOT EXISTS here.ta
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

ALTER TABLE here.ta OWNER TO here_admins;
GRANT SELECT ON TABLE here_staging.ta TO bdit_humans;
GRANT INSERT, UPDATE, SELECT, TRIGGER ON TABLE here_staging.ta TO here_bot;

-- Simple view structure 
CREATE VIEW here.ta_view AS
 
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

FROM here.ta

ALTER TABLE here.ta_view OWNER TO here_admins;
GRANT INSERT, UPDATE, SELECT, TRIGGER ON TABLE here_staging.ta TO here_bot;

  
-- Instead of inserting to ta_view, 
-- insert to parent table
CREATE TRIGGER transform_trigger
        INSTEAD OF INSERT
        ON here.ta_view
        FOR EACH ROW
        EXECUTE PROCEDURE here.here_insert_trigger();