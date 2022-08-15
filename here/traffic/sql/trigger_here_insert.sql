CREATE OR REPLACE FUNCTION here.here_insert_trigger()
    RETURNS trigger 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN 

INSERT INTO here.ta VALUES (NEW.link_dir, 
							NEW.tx::date,
							NEW.tx::time without time zone,
							NEW.tx,
							NEW.length,
							NEW.mean,
							NEW.stddev,
							NEW.min_spd,
							NEW.max_spd,
							NEW.confidence,
                            NEW.sample_size); -- postgresql <=10 does not support ON CONFLICT DO NOTHING
RETURN NULL;
END;
$BODY$;
