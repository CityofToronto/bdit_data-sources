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
		span integer,
		interpolated boolean);
	
	INSERT INTO bins
	SELECT 	intersection_uid, 
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin,
		COUNT(DISTINCT A.datetime_bin) AS avail_minutes,
		MIN(A.datetime_bin) AS start_time,
		MAX(A.datetime_bin) AS end_time,
		(EXTRACT(minutes FROM MAX(A.datetime_bin) - MIN(A.datetime_bin))::INT+1) AS span,
		NULL as interpolated
	FROM volumes A
	WHERE volume_15min_tmc_uid IS NULL
	GROUP BY intersection_uid, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900)
	HAVING COUNT(DISTINCT A.datetime_bin) > 5;
	
-- IF one of two 1-minute time bins BEFORE and AFTER 15-minute bin are populated, assume no interpolation needed
	UPDATE bins A 
	SET interpolated = FALSE
	FROM (SELECT DISTINCT intersection_uid, datetime_bin from miovision.volumes) B
	WHERE 	A.interpolated IS NULL 
		AND A.avail_minutes < 15 
		AND A.intersection_uid = B.intersection_uid 
		AND (B.datetime_bin >= (A.datetime_bin + INTERVAL '15 minutes') AND B.datetime_bin <= (A.datetime_bin + INTERVAL '16 minutes')) 
		AND (B.datetime_bin <= (A.datetime_bin - INTERVAL '1 minute') AND B.datetime_bin >= A.datetime_bin - (INTERVAL '2 minutes'));

	-- IF # of populated 1-minute bins exceeds difference between start and end time, assume no interpolation needed	
	UPDATE bins A
	SET interpolated = CASE
				WHEN 	(EXTRACT(minutes FROM A.end_time - A.start_time)+1) > A.avail_minutes AND A.avail_minutes < 15
				THEN 	FALSE
				WHEN 	interpolated IS NULL AND A.avail_minutes < 15	
	-- ASSUME for all other 15-minute bins with missing data, interpolation needed due to missing video
				THEN	TRUE
				END;

	-- FOR 15-minute bins with interpolation needed, IF missing data at start of 15-minute period, SET start_time = start_time + 1 minute to account for potential partial count
	-- FOR 15-minute bins with interpolation needed, IF missing data at end of 15-minute period, SET end_time = end_time - 1 minute to account for potential partial count
	UPDATE bins 
	SET end_time = CASE
		WHEN interpolated = TRUE AND datetime_bin = start_time THEN end_time - INTERVAL '1 minute' ELSE end_time
		END,
	start_time = CASE
		WHEN interpolated = TRUE AND datetime_bin + INTERVAL '14 minutes' = end_time THEN start_time + INTERVAL '1 minute' ELSE start_time
		END,
    span =  CASE
		WHEN interpolated = TRUE AND datetime_bin = start_time THEN span - 1
		WHEN interpolated = TRUE AND datetime_bin + INTERVAL '14 minutes' = end_time THEN span -1
		WHEN interpolated = TRUE AND datetime_bin = start_time 
			AND datetime_bin + INTERVAL '14 minutes' = end_time THEN span - 2 
 		ELSE span END;

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
			a volume record there and then will CASCADE up to the 0-volume aggregated rows */
		INSERT INTO volumes_tmc_zeroes 
		SELECT DISTINCT ON(a.volume_15min_tmc_uid) volume_uid, a.volume_15min_tmc_uid
		FROM aggregate_insert a
			INNER JOIN volumes B USING (intersection_uid)
			WHERE a.volume = 0 
			AND B.datetime_bin < A.datetime_bin +INTERVAL '15 minutes' AND B.datetime_bin>= A.datetime_bin
			ORDER BY a.volume_15min_tmc_uid
		)
			/*For the non-zero 15-min volumes, link the 1-min disaggregate volumes via FOREIGN KEY relationship*/
		UPDATE volumes a
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
GRANT EXECUTE ON FUNCTION miovision.aggregate_15_min_tmc() TO bdit_humans WITH GRANT OPTION;