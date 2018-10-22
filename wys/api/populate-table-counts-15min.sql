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
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin
		FROM 	wys.raw_data A
		GROUP BY api_id, datetime_bin
	)
	
	INSERT INTO 	bins
	SELECT 		api_id,
			datetime_bin,
			start_time,
			end_time
	FROM 		bin_grouping
	ORDER BY api_id, datetime_bin;



	-- INSERT INTO volumes_15min_tmc, with interpolated volumes
	WITH speed_bins AS (
		/*Cross product of legal movement for cars, bikes, and peds and the bins to aggregate*/
		SELECT api_id, datetime_bin, round(speed/5,0)*5 AS speed, sum(count) AS count
		FROM wys.raw_data
		GROUP BY api_id, datetime_bin, round(speed/5,0)*5
		)
	
		/*Inner join volume data with bins on intersection/datetime_bin then add zero padding for select movements*/
		--INSERT INTO miovision_api.volumes_15min_tmc(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
	INSERT INTO wys.counts_15min (api_id, datetime_bin, speed, count)
	SELECT 	B.api_id,
		B.datetime_bin,
		A.speed,
		sum(A.count) AS count
	FROM bins B
	INNER JOIN speed_bins A 
								ON A.api_id=B.api_id
								AND A.datetime_bin BETWEEN B.start_time AND B.end_time
	/*Only join the zero padding movements to the left side when everything matches, including the bin's datetime_bin
	Otherwise zero-pad*/

	GROUP BY B.api_id, B.datetime_bin, A.speed
	ORDER BY B.api_id, B.datetime_bin, A.speed

