CREATE OR REPLACE FUNCTION wys.insert_trigger()
RETURNS TRIGGER AS $$
BEGIN 

    IF (new.datetime_bin >= '2020-01-01' AND new.datetime_bin < '2020-01-01'::DATE + INTERVAL '1 year') THEN 
        INSERT INTO wys.raw_data_2020 VALUES (NEW.*) ON CONFLICT DO NOTHING; 
    ELSIF (new.datetime_bin >= '2019-01-01' AND new.datetime_bin < '2019-01-01'::DATE + INTERVAL '1 year') THEN 
        INSERT INTO wys.raw_data_2019 VALUES (NEW.*) ON CONFLICT DO NOTHING; 
    ELSIF (new.datetime_bin >= '2018-01-01' AND new.datetime_bin < '2018-01-01'::DATE + INTERVAL '1 year') THEN 
        INSERT INTO wys.raw_data_2018 VALUES (NEW.*) ON CONFLICT DO NOTHING; 
    ELSIF (new.datetime_bin >= '2017-01-01' AND new.datetime_bin < '2017-01-01'::DATE + INTERVAL '1 year') THEN 
        INSERT INTO wys.raw_data_2017 VALUES (NEW.*) ON CONFLICT DO NOTHING; 
    ELSE 
	    RAISE EXCEPTION 'datetime out of range. Fix the wys.insert_trigger() function!';
    END IF;
    RETURN NULL;
END;
$$ 
LANGUAGE plpgsql SECURITY DEFINER;


CREATE TRIGGER partition_raw_data_trigger
  BEFORE INSERT
  ON wys.raw_data
  FOR EACH ROW
  EXECUTE PROCEDURE wys.insert_trigger();