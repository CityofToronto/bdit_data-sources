CREATE OR REPLACE FUNCTION miovision_api.find_invalid_movements(
    start_date timestamp without time zone,
    end_date timestamp without time zone)
  RETURNS integer AS
$BODY$
DECLARE sum integer;
BEGIN

		SELECT sum(volume) INTO sum FROM miovision_api.volumes a 
		LEFT JOIN miovision_api.intersection_movements b USING (intersection_uid,leg, classification_uid, movement_uid)
		WHERE datetime_bin BETWEEN start_date AND end_date
		AND b.intersection_uid IS NULL;
		

		IF sum >1000 
			THEN RAISE NOTICE 'Invalid movements more than 1000, QC check raw data';
			RETURN 1;
		ELSE 
			RAISE NOTICE 'Number of invalid movements less than 1000';	
			RETURN 0;
		END IF;
END
;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

COMMENT ON FUNCTION miovision_api.find_invalid_movements(timestamp, timestamp) IS
'''Used exclusively within `intersection_tmc.py` `insert_data` function to raise
notice in the logs about invalid movements.''';