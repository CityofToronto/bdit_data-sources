-- Function: miovision_api.update_tmc_index(timestamp without time zone, timestamp without time zone)

-- DROP FUNCTION miovision_api.update_tmc_index(timestamp without time zone, timestamp without time zone);

CREATE OR REPLACE FUNCTION miovision_api.update_tmc_index(
    start_date timestamp without time zone,
    end_date timestamp without time zone)
  RETURNS integer AS
$BODY$
BEGIN

	UPDATE miovision_api.volumes a
	SET volume_15min_tmc_uid = b.volume_15min_tmc_uid
	FROM (SELECT * FROM miovision_api.volumes_15min_tmc WHERE datetime_bin BETWEEN start_date AND end_date) b
	WHERE a.volume_15min_tmc_uid IS NULL
	AND a.datetime_bin >= b.datetime_bin AND a.datetime_bin < b.datetime_bin + INTERVAL '15 minutes'
	AND a.classification_uid  = b.classification_uid 
	AND a.leg = b.leg
	AND a.movement_uid = b.movement_uid
	AND a.intersection_uid = b.intersection_uid;

    RETURN 1;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION miovision_api.update_tmc_index(timestamp without time zone, timestamp without time zone)
  OWNER TO rliu;
GRANT EXECUTE ON FUNCTION miovision_api.update_tmc_index(timestamp without time zone, timestamp without time zone) TO public;
GRANT EXECUTE ON FUNCTION miovision_api.update_tmc_index(timestamp without time zone, timestamp without time zone) TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION miovision_api.update_tmc_index(timestamp without time zone, timestamp without time zone) TO bdit_humans WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION miovision_api.update_tmc_index(timestamp without time zone, timestamp without time zone) TO rliu;
