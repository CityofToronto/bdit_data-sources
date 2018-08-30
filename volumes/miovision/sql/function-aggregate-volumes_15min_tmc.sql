-- FUNCTION: miovision.aggregate_15_min_tmc()

-- DROP FUNCTION miovision.aggregate_15_min_tmc();

CREATE OR REPLACE FUNCTION miovision.aggregate_15_min_tmc(
	)
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
			interpolated boolean);
			
	WITH class_grouping AS (

		SELECT 	intersection_uid, 
			datetime_bin as one_minute_bins,
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin,
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) - interval '1 minute' AS previous_bin,
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) + interval '15 minute' AS next_bin,
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) - interval '2 minute' AS previous_bin2,
			TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) + interval '16 minute' AS next_bin2
		FROM 	miovision.volumes A
		GROUP BY intersection_uid, datetime_bin	
	), bin_grouping AS(
		SELECT 	intersection_uid, 
			datetime_bin, 
			COUNT(DISTINCT one_minute_bins) AS avail_minutes,
			min(one_minute_bins) as start_time,
			max(one_minute_bins) as end_time,
			lag(max(one_minute_bins)) OVER w as previous_end,
			lead(min(one_minute_bins)) OVER w as next_start,
			previous_bin,
			next_bin,
			previous_bin2,
			next_bin2,
			CASE WHEN  COUNT(DISTINCT one_minute_bins)<15 
				AND (NULLIF(next_bin, lead(min(one_minute_bins)) OVER w) IS NULL OR NULLIF(next_bin2, lead(min(one_minute_bins)) OVER w) IS NULL)
				AND (NULLIF(previous_bin, lag(max(one_minute_bins)) OVER w ) IS NULL OR NULLIF(previous_bin2, lag(max(one_minute_bins)) OVER w ) IS NULL) 
				THEN FALSE 
				ELSE NULL 
				END AS interpolated
		FROM class_grouping a
		GROUP BY intersection_uid, datetime_bin, previous_bin, next_bin, previous_bin2, next_bin2
		WINDOW w AS (PARTITION BY intersection_uid)
		ORDER BY intersection_uid, datetime_bin
	)

	INSERT INTO 	bins
	SELECT 		intersection_uid,
			datetime_bin,
			avail_minutes,
			start_time,
			end_time,
			interpolated
	FROM 		bin_grouping
	WHERE avail_minutes>5
	ORDER BY intersection_uid, datetime_bin;


	UPDATE bins SET interpolated = FALSE WHERE (EXTRACT(minutes FROM end_time - start_time)+1) > avail_minutes AND avail_minutes < 15;
	UPDATE bins SET interpolated = TRUE WHERE interpolated IS NULL AND avail_minutes < 15;

	UPDATE bins
	SET 	end_time = CASE
		WHEN interpolated = TRUE AND datetime_bin = start_time THEN end_time - INTERVAL '1 minute' ELSE end_time
		END,
		start_time = CASE
		WHEN interpolated = TRUE AND datetime_bin + INTERVAL '14 minutes' = end_time THEN start_time + INTERVAL '1 minute' ELSE start_time
		END;

	BEGIN 
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
			/*Link each 0-volume 15-min TMC record to any *one* 1-min volume record at that same intersection/time-bin so that deleting
			a volume record there and then will CASCADE up to the 0-volume aggregated rows. Currently just uses minimum volume_uid for
			each bin */
			INSERT INTO volumes_tmc_zeroes 
			SELECT a_volume_uid, a.volume_15min_tmc_uid
			FROM aggregate_insert a
			INNER JOIN bins USING(intersection_uid, datetime_bin)
			WHERE a.volume = 0
		)
			/*For the non-zero 15-min volumes, link the 1-min disaggregate volumes via FOREIGN KEY relationship*/
		UPDATE miovision.volumes a
		SET volume_15min_tmc_uid = b.volume_15min_tmc_uid
		FROM aggregate_insert b
		WHERE b.volume > 0 
			AND a.intersection_uid  = b.intersection_uid 
			AND a.datetime_bin >= b.datetime_bin AND a.datetime_bin < b.datetime_bin + INTERVAL '15 minutes'
			AND a.classification_uid  = b.classification_uid 
			AND a.leg = b.leg
			AND a.movement_uid = b.movement_uid
		;
	EXCEPTION
		WHEN unique_violation THEN 
			RAISE EXCEPTION 'Attempting to aggregate data that has already been aggregated but not deleted';
			RETURN 0;
	END;
END;

$BODY$;

ALTER FUNCTION miovision.aggregate_15_min_tmc()
    OWNER TO dbadmin;

GRANT EXECUTE ON FUNCTION miovision.aggregate_15_min_tmc() TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION miovision.aggregate_15_min_tmc() TO bdit_humans;