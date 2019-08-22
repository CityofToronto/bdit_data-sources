
CREATE OR REPLACE FUNCTION miovision_api.trgr_volumes_delete()
RETURNS TRIGGER AS $$
BEGIN 
	DELETE FROM miovision_api.volumes_15min_tmc a
	WHERE a.volume_15min_tmc_uid = OLD.volume_15min_tmc_uid;
	RETURN OLD;
END $$
LANGUAGE 'plpgsql'
SECURITY DEFINER;

DROP TRIGGER IF EXISTS volumes_delete on miovision_api.volumes;
CREATE TRIGGER volumes_delete 
AFTER DELETE on miovision_api.volumes
FOR EACH ROW
EXECUTE PROCEDURE miovision_api.trgr_volumes_delete();

CREATE OR REPLACE FUNCTION miovision_api.trgr_volumes_tmc_zeroes_delete()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER 
AS $BODY$

BEGIN 
	DELETE FROM miovision_api.volumes_15min_tmc a
	WHERE a.volume_15min_tmc_uid = OLD.volume_15min_tmc_uid;
	RETURN OLD;
END 
$BODY$;
DROP TRIGGER IF EXISTS volumes_delete ON miovision_api.volumes_tmc_zeroes;
CREATE TRIGGER volumes_delete
    AFTER DELETE
    ON miovision_api.volumes_tmc_zeroes
    FOR EACH ROW
    EXECUTE PROCEDURE miovision_api.trgr_volumes_tmc_zeroes_delete();

CREATE OR REPLACE FUNCTION miovision_api.trgr_volumes_tmc_atr_delete()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER 
AS $BODY$

BEGIN 
	DELETE FROM miovision_api.volumes_15min a
	WHERE a.volume_15min_uid = OLD.volume_15min_uid;
	RETURN OLD;
END 
$BODY$;

DROP TRIGGER IF EXISTS atr_volumes_delete ON miovision_api.volumes_tmc_atr_xover;
CREATE TRIGGER atr_volumes_delete
    AFTER DELETE
    ON miovision_api.volumes_tmc_atr_xover
    FOR EACH ROW
    EXECUTE PROCEDURE miovision_api.trgr_volumes_tmc_atr_delete();