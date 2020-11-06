SET SCHEMA 'miovision';

CREATE FUNCTION miovision_api.volumes_insert_trigger()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF 
AS $BODY$
BEGIN
	IF new.datetime_bin >= '2018-01-01'::date AND new.datetime_bin < ('2018-01-01'::date + '1 year'::interval) THEN 
		INSERT INTO miovision_api.volumes_2018 (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume) 
		VALUES (NEW.*);
	ELSIF new.datetime_bin >= '2019-01-01'::date AND new.datetime_bin < ('2019-01-01'::date + '1 year'::interval) THEN 
		INSERT INTO miovision_api.volumes_2019 (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume) 
		VALUES (NEW.intersection_uid, NEW.datetime_bin, NEW.classification_uid, NEW.leg, NEW.movement_uid, NEW.volume);
	ELSIF new.datetime_bin >= '2020-01-01'::date AND new.datetime_bin < ('2020-01-01'::date + '1 year'::interval) THEN 
		INSERT INTO miovision_api.volumes_2020 (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume) 
		VALUES (NEW.intersection_uid, NEW.datetime_bin, NEW.classification_uid, NEW.leg, NEW.movement_uid, NEW.volume);
  ELSE 
    RAISE EXCEPTION 'Datetime_bin out of range.  Fix the volumes_insert_trigger() function!';
	END IF;
	RETURN NULL;
EXCEPTION
    WHEN UNIQUE_VIOLATION THEN 
        RAISE WARNING 'You are trying to insert duplicate data!';
    RETURN NULL;
END;
$BODY$;
