

CREATE OR REPLACE FUNCTION find_invalid_movements()
  RETURNS integer AS
$BODY$
DECLARE sum integer;
BEGIN


		SELECT sum(volume) INTO sum FROM miovision.volumes
		WHERE datetime_bin::date BETWEEN now()::date- INTERVAL '30 days' AND now()::Date

		AND (

		(intersection_uid IN (2,3,4)
		AND leg='E' 
		AND classification_uid IN (1,3,4,5))

		OR (intersection_uid IN (27,28,29,31,1,5)
		AND leg='W' 
		AND classification_uid IN (1,3,4,5))

		OR (intersection_uid IN (14)
		AND leg='S' 
		AND classification_uid IN (1,3,4,5))

		OR (intersection_uid IN (16)
		AND leg='N' 
		AND classification_uid IN (1,3,4,5))

		OR (intersection_uid IN (26)
		AND classification_uid IN (1,3,4,5)
		AND ((leg IN ('E','W') 
		AND movement_uid IN (1,4))
		OR (leg IN ('N','S') 
		AND movement_uid IN (2,3))))

		OR (intersection_uid IN (30)
		AND leg IN ('W') 
		AND classification_uid IN (1,3,4,5)
		AND movement_uid IN (1,4))

		OR (intersection_uid IN (27,28,29,31,30)
		AND classification_uid IN (1,3,4,5)
		AND ((leg IN ('S') 
		AND movement_uid IN (3)) OR (leg IN ('N') 
		AND movement_uid IN (2))))

		OR (intersection_uid IN (2,3,4)
		AND classification_uid IN (1,3,4,5)
		AND ((leg IN ('S') 
		AND movement_uid IN (2)) OR (leg IN ('N') 
		AND movement_uid IN (3))))

		OR (intersection_uid IN (14)
		AND classification_uid IN (1,3,4,5)
		AND ((leg IN ('E') 
		AND movement_uid IN (3)) OR (leg IN ('W') 
		AND movement_uid IN (2))))

		OR (intersection_uid IN (16)
		AND classification_uid IN (1,3,4,5)
		AND ((leg IN ('E') 
		AND movement_uid IN (2)) OR (leg IN ('W') 
		AND movement_uid IN (3)))));

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
