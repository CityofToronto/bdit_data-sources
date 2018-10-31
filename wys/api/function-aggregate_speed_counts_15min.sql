CREATE OR REPLACE FUNCTION wys.aggregate_speed_counts_15min()
  RETURNS integer AS
$BODY$

BEGIN
	DROP TABLE IF EXISTS bins;

	CREATE TEMPORARY TABLE bins (
			api_id integer,
			datetime_bin timestamp without time zone,
			start_time timestamp without time zone,
			end_time timestamp without time zone
			);
			
	WITH bin_grouping AS (

		SELECT 	api_id, 
			min(datetime_bin) AS start_time,
			max(datetime_bin) AS end_time,
			count(DISTINCT(datetime_bin)) AS avail_bins,
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin
		FROM 	wys.raw_data A
		WHERE	counts_15min IS NULL
		GROUP BY api_id, datetime_bin
	)
	
	INSERT INTO 	bins
	SELECT 		api_id,
			datetime_bin,
			start_time,
			end_time
	FROM 		bin_grouping
	WHERE		avail_bins>1
	ORDER BY api_id, datetime_bin;

	WITH speed_bins AS (
	SELECT api_id, datetime_bin, speed_id, sum(count) AS count
	FROM wys.raw_data B
	CROSS JOIN wys.speed_bins A 
	WHERE b.speed<@a.speed_bin
	GROUP BY api_id, datetime_bin, speed_id

	), insert_data AS ( 

	INSERT INTO wys.counts_15min (api_id, datetime_bin, speed_id, count)
	SELECT 	B.api_id,
		B.datetime_bin,
		A.speed_id,
		sum(A.count) AS count
	FROM bins B
	INNER JOIN speed_bins A 
							ON A.api_id=B.api_id
							AND A.datetime_bin BETWEEN B.start_time AND B.end_time
	GROUP BY B.api_id, B.datetime_bin, A.speed_id
	ORDER BY B.api_id, B.datetime_bin, A.speed_id
	RETURNING counts_15min, api_id, datetime_bin)

	UPDATE wys.raw_data A
	SET counts_15min=B.counts_15min
	FROM insert_data B
	WHERE A.counts_15min IS NULL
	AND A.api_id=B.api_id
	AND A.datetime_bin >= B.datetime_bin AND A.datetime_bin < B.datetime_bin + INTERVAL '15 minutes' ;
	RETURN 1;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;