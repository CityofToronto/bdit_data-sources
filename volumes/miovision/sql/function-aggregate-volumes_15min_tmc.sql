SET SCHEMA 'miovision';

CREATE OR REPLACE FUNCTION aggregate_15_min_tmc()
  RETURNS void AS
$BODY$

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
			min(datetime_bin) as start_time,
			max(datetime_bin) as end_time,
			lag(max(datetime_bin)) OVER w as previous_end,
			lead(min(datetime_bin)) OVER w as next_start,
			(EXTRACT(minutes FROM MAX(datetime_bin) - MIN(datetime_bin))::INT+1) AS span,
			MIN(volume_uid) as a_volume_uid,
			CASE WHEN  COUNT(DISTINCT datetime_bin) < 15 --If there is at least 1 missing one-minute bin in the current 15-min bin
				/*and at least one one-minute bin in the following 2 minute*/
				AND lead(min(datetime_bin)) OVER w <= datetime_bin_15(datetime_bin) + interval '16 minutes'
				/*and at least one one-minute bin in the preceding 2 minute*/
				AND lag(max(datetime_bin)) OVER w  >= datetime_bin_15(datetime_bin) - interval '2 minutes' 
				THEN FALSE 
				ELSE NULL 
				END AS interpolated
		FROM volumes A
		INNER JOIN miovision.intersection_movements	m  --Make sure movement is valid.
		 											USING (intersection_uid, classification_uid,leg,movement_uid)
		WHERE volume_15min_tmc_uid IS NULL
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
		FROM miovision.intersection_movements m
		INNER JOIN bins USING (intersection_uid)
		WHERE classification_uid IN (1,2,6,7) 
		)
	,aggregate_insert AS(
		/*Inner join volume data with bins on intersection/datetime_bin then add zero padding for select movements*/
		INSERT INTO volumes_15min_tmc(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
		SELECT 	COALESCE(C.intersection_uid, A.intersection_uid) intersection_uid,
			COALESCE(C.datetime_bin, B.datetime_bin) datetime_bin,
			COALESCE(A.classification_uid, C.classification_uid) classification_uid,
			COALESCE(A.leg, C.leg) leg,
			COALESCE(A.movement_uid, C.movement_uid) movement_uid,
			COALESCE(CASE WHEN B.interpolated = TRUE THEN SUM(A.volume)*15.0/(span*1.0) ELSE SUM(A.volume) END, 0) AS volume
		FROM bins B
		INNER JOIN volumes A ON A.volume_15min_tmc_uid IS NULL
									AND B.intersection_uid = A.intersection_uid 
									AND B.start_time <= A.datetime_bin AND B.end_time >= A.datetime_bin
		INNER JOIN miovision.intersection_movements	m ON --Make sure movement is valid.
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
		GROUP BY COALESCE(C.intersection_uid, A.intersection_uid), COALESCE(C.datetime_bin, B.datetime_bin), 
				 COALESCE(A.classification_uid, C.classification_uid), COALESCE( A.leg, C.leg), 
				 COALESCE(A.movement_uid, C.movement_uid), interpolated, span
		RETURNING intersection_uid, volume_15min_tmc_uid, datetime_bin, classification_uid, leg, movement_uid, volume
	)
    , zero_insert AS(
	INSERT INTO volumes_tmc_zeroes 
	SELECT a_volume_uid, a.volume_15min_tmc_uid
	FROM aggregate_insert a
	INNER JOIN bins USING(intersection_uid, datetime_bin)
		WHERE a.volume = 0
	)
	UPDATE volumes a
	SET volume_15min_tmc_uid = b.volume_15min_tmc_uid
	FROM aggregate_insert b
	WHERE a.volume_15min_tmc_uid IS NULL AND b.volume > 0 
	AND a.intersection_uid  = b.intersection_uid 
	AND a.datetime_bin >= b.datetime_bin AND a.datetime_bin < b.datetime_bin + INTERVAL '15 minutes'
	AND a.classification_uid  = b.classification_uid 
	AND a.leg = b.leg
	AND a.movement_uid = b.movement_uid
     ;
	RAISE NOTICE '% Done', timeofday();
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;

GRANT EXECUTE ON FUNCTION aggregate_15_min_tmc() TO dbadmin WITH GRANT OPTION;

GRANT EXECUTE ON FUNCTION aggregate_15_min_tmc() TO bdit_humans WITH GRANT OPTION;