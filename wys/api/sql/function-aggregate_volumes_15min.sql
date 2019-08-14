-- Function: wys.aggregate_volumes_15min()

-- DROP FUNCTION wys.aggregate_volumes_15min();

CREATE OR REPLACE FUNCTION wys.aggregate_volumes_15min()
  RETURNS integer AS
$BODY$

BEGIN
	WITH insert_data AS (

		INSERT INTO wys.volumes_15min (api_id, datetime_bin, count)
		SELECT 	C.api_id, 
			C.datetime_bin,
			sum(C.count)
		FROM 	wys.counts_15min C
		WHERE volumes_15min_uid IS NULL
		GROUP BY api_id, datetime_bin
		ORDER BY api_id, datetime_bin
		RETURNING api_id, datetime_bin, volumes_15min_uid)
	
	UPDATE wys.counts_15min A
	SET volumes_15min_uid=B.volumes_15min_uid
	FROM insert_data B
	WHERE A.volumes_15min_uid IS NULL
	AND A.api_id=B.api_id
	AND A.datetime_bin = B.datetime_bin;
	RETURN 1;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION wys.aggregate_volumes_15min()
  OWNER TO rliu;
GRANT EXECUTE ON FUNCTION wys.aggregate_volumes_15min() TO public;
GRANT EXECUTE ON FUNCTION wys.aggregate_volumes_15min() TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION wys.aggregate_volumes_15min() TO bdit_humans WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION wys.aggregate_volumes_15min() TO rliu;
