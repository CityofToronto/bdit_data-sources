CREATE OR REPLACE FUNCTION wys.aggregate_volumes_15min()
  RETURNS integer AS
$BODY$

BEGIN
	WITH valid_bins AS(
		SELECT 	A.api_id,
			A.datetime_bin::date + B.time::time without time zone AS  datetime_bin
		FROM	(SELECT DISTINCT api_id, datetime_bin::date FROM wys.counts_15min) A
		CROSS JOIN generate_series('2017-01-01 06:00:00'::timestamp without time zone, '2017-01-01 19:45:00'::timestamp without time zone, '00:15:00'::interval) B
		ORDER BY A.api_id, (A.datetime_bin::date + B.time::time without time zone)
	), insert_data AS (

		INSERT INTO wys.volumes_15min (api_id, datetime_bin, count)
		SELECT 	C.api_id, 
			C.datetime_bin, 
			COALESCE(sum(D.count), 0) AS count 
		FROM 	valid_bins C
		LEFT JOIN wys.counts_15min D USING (api_id, datetime_bin)
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

