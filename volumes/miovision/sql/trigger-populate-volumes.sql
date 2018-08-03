CREATE OR REPLACE FUNCTION insert_volumes()
  RETURNS trigger AS
$BODY$

BEGIN

	WITH new_data AS(
	   

		SELECT B.intersection_uid, (NEW.datetime_bin AT TIME ZONE 'America/Toronto') AS datetime_bin, NEW.classification, NEW.entry_dir_name as leg, NEW.movement, NEW.volume
		FROM miovision.intersections B 
		WHERE regexp_replace(NEW.study_name,'Yong\M','Yonge') = B.intersection_name)
		INSERT INTO rliu.volumes (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
	 
	SELECT A.intersection_uid, A.datetime_bin, C.classification_uid, A.leg, D.movement_uid, A.volume 
	FROM new_data A
	INNER JOIN miovision.movements D USING (movement)
	INNER JOIN miovision.classifications C USING (classification, location_only);
	RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION insert_volumes()
  OWNER TO rliu;
GRANT EXECUTE ON FUNCTION insert_volumes() TO public;
GRANT EXECUTE ON FUNCTION insert_volumes() TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION insert_volumes() TO bdit_humans WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION insert_volumes() TO rliu;

CREATE TRIGGER insert_volumes_trigger
  AFTER INSERT
  ON raw_data
  FOR EACH ROW
  EXECUTE PROCEDURE insert_volumes();
