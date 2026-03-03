-- FUNCTION: here.here_insert_trigger_hm_path()

-- DROP FUNCTION IF EXISTS here.here_insert_trigger_hm_path();

CREATE OR REPLACE FUNCTION here.here_insert_trigger_hm_path()
RETURNS trigger
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN 

INSERT INTO here.ta_path_hm (
    link_dir, tx, mean, harmonic_mean, stddev, min_spd, max_spd, pct_50, pct_85, sample_size
)
VALUES (
    NEW.link_dir,
    NEW.tx,
    NEW.mean,
    NEW.harmonic_mean,
    NEW.stddev,
    NEW.min_spd,
    NEW.max_spd,
    NEW.pct_50,
    NEW.pct_85,
    NEW.sample_size
); 
RETURN NULL;
END;
$BODY$;

ALTER FUNCTION here.here_insert_trigger_hm_path()
OWNER TO here_admins;
