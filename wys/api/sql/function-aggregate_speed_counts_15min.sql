-- Function: wys.aggregate_speed_counts_15min()

-- DROP FUNCTION wys.aggregate_speed_counts_15min();

CREATE OR REPLACE FUNCTION wys.aggregate_speed_counts_15min()
  RETURNS integer AS
$BODY$

BEGIN
	DROP TABLE IF EXISTS bins;

	--bins is a table with the valid 15 minute bins for each intersection
	
	CREATE TEMPORARY TABLE bins (
			api_id integer,
			datetime_bin timestamp without time zone,
			start_time timestamp without time zone,
			end_time timestamp without time zone
			);
			
	WITH bin_grouping AS (

		SELECT DISTINCT	api_id, 

			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) - INTERVAL '1 minute' AS datetime_bin,
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) - INTERVAL '1 minute' AS start_time,
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) + INTERVAL '14 minute' AS end_time
		FROM 	wys.speed_counts A
		--WHERE	counts_15min IS NULL
		GROUP BY api_id, datetime_bin
	)
	
	INSERT INTO 	bins
	SELECT 		api_id,
			datetime_bin,
			start_time,
			end_time
	FROM 		bin_grouping
	ORDER BY api_id, datetime_bin;

	--Aggregates data that are in the same speed bin together. Speed IDs found in python

	WITH speed_bins AS (
	SELECT api_id, datetime_bin, speed_id, sum(count) AS count
	FROM wys.speed_counts B
	WHERE	counts_15min IS NULL
	GROUP BY api_id, datetime_bin, speed_id

	), insert_data AS(

	--Joins aggregated speed bin data to the valid bins and inserts to counts_15min

	INSERT INTO wys.counts_15min (api_id, datetime_bin, speed_id, count)
	SELECT 	B.api_id,
		B.datetime_bin,
		A.speed_id,
		sum(A.count) AS count
	FROM bins B
	INNER JOIN speed_bins A 
							ON A.api_id=B.api_id
							WHERE A.datetime_bin >= B.start_time 
							AND A.datetime_bin < B.end_time
	GROUP BY B.api_id, B.datetime_bin, A.speed_id
	ORDER BY B.api_id, B.datetime_bin, A.speed_id
	RETURNING counts_15min, api_id, datetime_bin, speed_id)

	--Sets IDs to speed_counts

	UPDATE wys.speed_counts A
	SET counts_15min=B.counts_15min
	FROM insert_data B
	WHERE A.counts_15min IS NULL
	AND A.api_id=B.api_id
	AND A.datetime_bin >= B.datetime_bin AND A.datetime_bin < B.datetime_bin + INTERVAL '15 minutes' 
	AND A.speed_id=B.speed_id;
	RETURN 1;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION wys.aggregate_speed_counts_15min()
  OWNER TO rliu;
GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_15min() TO public;
GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_15min() TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_15min() TO bdit_humans WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_15min() TO rliu;
