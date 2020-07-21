CREATE OR REPLACE FUNCTION miovision_api.aggregate_15_min_tmc_test_gap(
	start_date date,
	end_date date)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

BEGIN
	DROP TABLE IF EXISTS bins;

	CREATE TEMPORARY TABLE bins (
			intersection_uid integer,
			datetime_bin timestamp without time zone,
			avail_minutes integer,
			start_time timestamp without time zone,
			end_time timestamp without time zone,
			span integer,
			interpolated boolean,
			a_volume_uid int);
			
	WITH  bin_grouping AS(
		SELECT 	intersection_uid, 
			datetime_bin_15(datetime_bin) AS datetime_bin_15, 
			COUNT(DISTINCT datetime_bin) AS avail_minutes,
			min(datetime_bin) AS start_time,
			max(datetime_bin) AS end_time,
			lag(max(datetime_bin)) OVER w AS previous_end,
			lead(min(datetime_bin)) OVER w AS next_start,
			(EXTRACT(minutes FROM MAX(datetime_bin) - MIN(datetime_bin))::INT+1) AS span,
			MIN(volume_uid) AS a_volume_uid,
			CASE WHEN  COUNT(DISTINCT datetime_bin) < 15 --If there is at least 1 missing one-minute bin in the current 15-min bin
				/*and at least one one-minute bin in the following 2 minute*/
				AND lead(min(datetime_bin)) OVER w <= datetime_bin_15(datetime_bin) + interval '16 minutes'
				/*and at least one one-minute bin in the preceding 2 minute*/
				AND lag(max(datetime_bin)) OVER w  >= datetime_bin_15(datetime_bin) - interval '2 minutes' 
				THEN FALSE 
				ELSE NULL 
				END AS interpolated
		FROM miovision_api.volumes_test A
		INNER JOIN miovision_api.intersection_movements_new	m  --Make sure movement is valid.
		 USING (intersection_uid, classification_uid,leg,movement_uid)
		WHERE datetime_bin >= start_date - INTERVAL '2 hours' AND datetime_bin < end_date 
		AND volume_15min_tmc_uid IS NULL --not processed yet
		AND volume_uid NOT IN (SELECT volume_uid FROM miovision_api.intersection_outages) 
		--Excluding time bins with unacceptable gaps
		GROUP BY intersection_uid, datetime_bin_15
		WINDOW w AS (PARTITION BY intersection_uid)
	)

	INSERT INTO 	bins
	SELECT 		intersection_uid,
			datetime_bin_15,
			avail_minutes,
			start_time,
			end_time,
			span,
			interpolated,
			a_volume_uid
	FROM 		bin_grouping
	WHERE avail_minutes>5;

	UPDATE bins SET interpolated = FALSE WHERE (EXTRACT(minutes FROM end_time - start_time)+1) > avail_minutes AND avail_minutes < 15;
	UPDATE bins SET interpolated = TRUE WHERE interpolated IS NULL AND avail_minutes < 15;

	UPDATE bins
	SET 	end_time = CASE
		WHEN datetime_bin = start_time THEN end_time - INTERVAL '1 minute' ELSE end_time
		END,
		start_time = CASE
		WHEN datetime_bin + INTERVAL '14 minutes' = end_time THEN start_time + INTERVAL '1 minute' ELSE start_time
		END,
		span =  CASE
		WHEN datetime_bin = start_time THEN span - 1
		WHEN datetime_bin + INTERVAL '14 minutes' = end_time THEN span -1
		WHEN datetime_bin = start_time 
			AND datetime_bin + INTERVAL '14 minutes' = end_time THEN span - 2 
 		ELSE span END
		WHERE interpolated = TRUE ;
	RAISE NOTICE '% Interpolation finished', timeofday();
	
	-- INSERT INTO volumes_15min_tmc, with interpolated volumes
	WITH zero_padding_movements AS (
		/*Cross product of legal movement for cars, bikes, and peds and the bins to aggregate*/
		SELECT m.*, datetime_bin 
		FROM miovision_api.intersection_movements_new m
		CROSS JOIN generate_series(start_date - interval '1 hour', end_date - interval '1 hour 15 minutes', INTERVAL '15 minutes') AS dt(datetime_bin)
		WHERE classification_uid IN (1,2,6) 
		AND intersection_uid IN (SELECT intersection_uid FROM miovision_api.intersections_new 
                        WHERE start_date::date > date_installed 
                        AND date_decommissioned IS NULL)
		)
	,aggregate_insert AS(
		/*Inner join volume data with bins on intersection/datetime_bin then add zero padding for select movements*/
		INSERT INTO miovision_api.volumes_15min_tmc_test(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
		SELECT 	COALESCE(C.intersection_uid, A.intersection_uid) intersection_uid,
			COALESCE(C.datetime_bin, B.datetime_bin) datetime_bin,
			COALESCE(A.classification_uid, C.classification_uid) classification_uid,
			COALESCE(A.leg, C.leg) leg,
			COALESCE(A.movement_uid, C.movement_uid) movement_uid,
			COALESCE(CASE WHEN B.interpolated = TRUE THEN SUM(A.volume)*15.0/(span*1.0) ELSE SUM(A.volume) END, 0) AS volume
		FROM bins B
		INNER JOIN miovision_api.volumes_test A ON A.volume_15min_tmc_uid IS NULL
									AND B.intersection_uid = A.intersection_uid 
									AND B.start_time <= A.datetime_bin AND B.end_time >= A.datetime_bin
									AND A.datetime_bin >= start_date - INTERVAL '1 hour' 
									AND A.datetime_bin < end_date - INTERVAL '1 hour'
		INNER JOIN miovision_api.intersection_movements_new	m ON --Make sure movement is valid.
		 											m.intersection_uid = A.intersection_uid
												AND m.classification_uid  = A.classification_uid 
												AND m.leg = A.leg
												AND m.movement_uid = A.movement_uid
		/*Only join the zero padding movements to the left side when everything matches, including the bin's datetime_bin
		Otherwise zero-pad*/
		FULL OUTER JOIN zero_padding_movements C ON C.intersection_uid = A.intersection_uid
												AND C.classification_uid  = A.classification_uid 
												AND C.leg = A.leg
												AND C.movement_uid = A.movement_uid
												AND C.datetime_bin = B.datetime_bin
		--WHERE B.datetime_bin >= start_date - INTERVAL '1 hour' AND B.datetime_bin < end_date - INTERVAL '1 hour'
		GROUP BY COALESCE(C.intersection_uid, A.intersection_uid), COALESCE(C.datetime_bin, B.datetime_bin), 
				 COALESCE(A.classification_uid, C.classification_uid), COALESCE( A.leg, C.leg), 
				 COALESCE(A.movement_uid, C.movement_uid), interpolated, span
		RETURNING intersection_uid, volume_15min_tmc_uid, datetime_bin, classification_uid, leg, movement_uid, volume
	)
	--to set unacceptable gap volume to null
	, set_null_vol AS (
	UPDATE miovision_api.volumes_15min_tmc_test
	SET volume = NULL
	WHERE volume_15min_tmc_uid IN 
	(SELECT volume_15min_tmc_uid
	FROM miovision_api.volumes_15min_tmc_test vol
	INNER JOIN 
	(
	SELECT DISTINCT DATE_TRUNC('hour', datetime_bin) AS null_hour, intersection_uid
	FROM miovision_api.intersection_outages
	WHERE datetime_bin >= start_date - INTERVAL '2 hours' AND datetime_bin < end_date 
	ORDER BY null_hour, intersection_uid
		) nul
	ON vol.intersection_uid = nul.intersection_uid
	AND DATE_TRUNC('hour', vol.datetime_bin) = nul.null_hour )
	)
    , zero_insert AS(
	INSERT INTO miovision_api.volumes_tmc_zeroes_test 
	SELECT a_volume_uid, a.volume_15min_tmc_uid
	FROM aggregate_insert a
	INNER JOIN bins USING(intersection_uid, datetime_bin)
		WHERE a.volume = 0
	)
	UPDATE miovision_api.volumes_test a
	SET volume_15min_tmc_uid = b.volume_15min_tmc_uid
	FROM aggregate_insert b
	WHERE a.datetime_bin >= start_date - interval '1 hour' AND a.datetime_bin < end_date -  interval '1 hour'
	AND a.volume_15min_tmc_uid IS NULL AND b.volume > 0 
	AND a.intersection_uid  = b.intersection_uid 
	AND a.datetime_bin >= b.datetime_bin AND a.datetime_bin < b.datetime_bin + INTERVAL '15 minutes'
	AND a.classification_uid  = b.classification_uid 
	AND a.leg = b.leg
	AND a.movement_uid = b.movement_uid
     ;
	RAISE NOTICE '% Done', timeofday();
END;

$BODY$;
