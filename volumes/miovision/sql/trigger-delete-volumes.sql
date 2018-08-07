CREATE OR REPLACE FUNCTION miovision.trgr_volumes_delete()
RETURNS TRIGGER AS $$
BEGIN 
	DELETE FROM miovision.volumes_15min_tmc a
	WHERE a.volume_15min_tmc_uid = OLD.volume_15min_tmc_uid;
	RETURN OLD;
END $$
LANGUAGE 'plpgsql';
DROP TRIGGER IF EXISTS volumes_delete on miovision.volumes;
CREATE TRIGGER volumes_delete 
AFTER DELETE on miovision.volumes
FOR EACH ROW
EXECUTE PROCEDURE miovision.trgr_volumes_delete();

CREATE OR REPLACE FUNCTION miovision.trgr_volumes_15min_tmc_delete()
RETURNS TRIGGER AS $$
BEGIN 
	DELETE FROM miovision.volumes_15min a
	WHERE a.volume_15min_uid = OLD.volume_15min_uid;
	RETURN OLD;
END $$
LANGUAGE 'plpgsql';
DROP TRIGGER IF EXISTS volumes_delete on miovision.volumes_15min_tmc;
CREATE TRIGGER volumes_delete 
AFTER DELETE on miovision.volumes_15min_tmc
FOR EACH ROW
EXECUTE PROCEDURE miovision.trgr_volumes_15min_tmc_delete();