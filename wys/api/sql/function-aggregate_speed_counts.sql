

CREATE OR REPLACE FUNCTION wys.aggregate_speed_counts_one_hour()
  RETURNS integer AS
$BODY$

BEGIN
	

	WITH insert_data AS (
		--Aggregated into speed bins and 1 hour bin
		INSERT INTO wys.speed_counts_agg (api_id, datetime_bin, speed_id, count)
		SELECT api_id, date_trunc('hour', datetime_bin) dt, speed_id, sum(count) AS count
		FROM wys.raw_data
		INNER JOIN wys.speed_bins ON speed <@ speed_bin
		WHERE	speed_count_uid IS NULL
		GROUP BY api_id, dt,  new_speed_id 
		RETURNING speed_counts_agg_id, api_id, datetime_bin, speed_id
		)
        
	
	UPDATE wys.raw_data A
	SET speed_count_uid=B.speed_counts_agg_id
	FROM insert_data B
	WHERE A.counts_15min IS NULL
	AND A.api_id=B.api_id
	AND A.datetime_bin >= B.datetime_bin AND A.datetime_bin < B.datetime_bin + INTERVAL '1 hour';


	RETURN 1;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;

GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour() to wys_bot;