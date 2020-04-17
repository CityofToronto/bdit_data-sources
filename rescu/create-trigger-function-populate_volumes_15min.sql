CREATE FUNCTION jchew.insert_rescu_volumes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$

BEGIN

INSERT INTO jchew.rescu_volumes_15min (detector_id, datetime_bin, volume_15min, arterycode)

WITH raw_data AS (	
	SELECT 	TRIM(SUBSTRING(NEW.raw_info, 15, 12)) AS detector_id,
			dt + LEFT(NEW.raw_info,6)::time AS datetime_bin,
			nullif(TRIM(SUBSTRING(NEW.raw_info, 27, 10)),'')::int AS volume_15min
)

SELECT 	detector_id,
		a.datetime_bin,
		a.volume_15min,
		b.arterycode
FROM	raw_data a
LEFT JOIN rescu.detector_inventory b USING (detector_id)
WHERE a.volume_15min >= 0
ORDER BY datetime_bin, detector_id ;
RETURN NULL; -- result is ignored since this is an AFTER trigger

END;
$BODY$;

ALTER FUNCTION jchew.insert_rescu_volumes()
    OWNER TO jchew;

GRANT EXECUTE ON FUNCTION jchew.insert_rescu_volumes() TO PUBLIC;

GRANT EXECUTE ON FUNCTION jchew.insert_rescu_volumes() TO bdit_humans WITH GRANT OPTION;

GRANT EXECUTE ON FUNCTION jchew.insert_rescu_volumes() TO jchew;