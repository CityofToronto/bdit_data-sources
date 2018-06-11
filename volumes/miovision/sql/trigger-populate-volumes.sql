CREATE OR REPLACE FUNCTION miovision.insert_volumes_trigger()
  RETURNS trigger AS
$BODY$
BEGIN
    INSERT INTO miovision.volumes (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
    SELECT B.intersection_uid, (NEW.datetime_bin AT TIME ZONE 'America/Toronto') AS datetime_bin, C.classification_uid, NEW.entry_dir_name as leg, D.movement_uid, NEW.volume
    FROM miovision.intersections B 
    INNER JOIN miovision.movements D USING (movement)
    INNER JOIN miovision.classifications C USING (classification, location_only)
    WHERE regexp_replace(NEW.study_name,'Yong\M','Yonge') = B.intersection_name
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
CREATE TRIGGER insert_volumes
  BEFORE INSERT
  ON miovision.raw_data
  FOR EACH ROW
  EXECUTE PROCEDURE miovision.insert_volumes_trigger();
