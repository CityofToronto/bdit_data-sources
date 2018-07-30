-- Function: aggregate_15_min_tmc3()

-- DROP FUNCTION aggregate_15_min_tmc3();

CREATE OR REPLACE FUNCTION aggregate_15_min_tmc3()
  RETURNS integer AS
$BODY$
BEGIN

	RAISE NOTICE '% Function Start', timeofday();
	DROP TABLE IF EXISTS bins;
	CREATE TEMPORARY TABLE bins (
		intersection_uid integer,
		datetime_bin timestamp without time zone,
		avail_minutes integer,
		total_volume bigint,
		start_time timestamp without time zone,
		end_time timestamp without time zone,
		interpolated boolean);

	INSERT INTO bins
	SELECT 	intersection_uid, 
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin,
		COUNT(DISTINCT A.datetime_bin) AS avail_minutes,
		SUM(A.volume) AS total_volume,
		MIN(A.datetime_bin) AS start_time,
		MAX(A.datetime_bin) AS end_time,
		NULL as interpolated
	FROM rliu.volumes A
	WHERE volume_15min_tmc_uid IS NULL
	GROUP BY intersection_uid, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900)
	-- REMOVE ALL 15-minute bins where 5 or less of the 1-minute bins are populated with volume
	HAVING COUNT(DISTINCT A.datetime_bin) > 5;
	RAISE NOTICE '% Insert Finished', timeofday();
	

	-- IF one of two 1-minute time bins BEFORE and AFTER 15-minute bin are populated, assume no interpolation needed

	UPDATE bins A 
	SET interpolated = FALSE
	FROM (SELECT DISTINCT intersection_uid, datetime_bin FROM rliu.volumes WHERE volume_15min_tmc_uid IS NULL) B 
	WHERE A.interpolated IS NULL 
	AND A.avail_minutes < 15 
	AND A.intersection_uid = B.intersection_uid 
	AND (B.datetime_bin BETWEEN (A.datetime_bin + INTERVAL '15 minutes') AND  (A.datetime_bin + INTERVAL '16 minutes')
	OR B.datetime_bin BETWEEN(A.datetime_bin - INTERVAL '1 minute') AND (A.datetime_bin - (INTERVAL '2 minutes')));

	-- IF # of populated 1-minute bins exceeds difference between start and end time, assume no interpolation needed	

	RAISE NOTICE '% SELECT Distinct Bins', timeofday();
	UPDATE bins A
	SET interpolated = CASE
				WHEN 	(EXTRACT(minutes FROM A.end_time - A.start_time)+1) > A.avail_minutes AND A.avail_minutes < 15
				THEN 	FALSE
				WHEN 	interpolated IS NULL AND A.avail_minutes < 15	
	-- ASSUME for all other 15-minute bins with missing data, interpolation needed due to missing video
				THEN	TRUE
				END;

	RAISE NOTICE '% Rest of interpolated conditional', timeofday();

	

	-- FOR 15-minute bins with interpolation needed, IF missing data at start of 15-minute period, SET start_time = start_time + 1 minute to account for potential partial count
	-- FOR 15-minute bins with interpolation needed, IF missing data at end of 15-minute period, SET end_time = end_time - 1 minute to account for potential partial count
	UPDATE bins 
	SET end_time = CASE
		WHEN interpolated = TRUE AND datetime_bin = start_time THEN end_time - INTERVAL '1 minute' ELSE end_time
		END,
	start_time = CASE
		WHEN interpolated = TRUE AND datetime_bin + INTERVAL '14 minutes' = end_time THEN start_time + INTERVAL '1 minute' ELSE start_time
		END;
		RAISE NOTICE '% Adjusting Interpolated Bins', timeofday();
	

	-- INSERT INTO volumes_15min_tmc, with interpolated volumes
	WITH aggregate_insert AS(
	INSERT INTO rliu.volumes_15min_tmc(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
	SELECT 	A.intersection_uid,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin,
		A.classification_uid,
		A.leg,
		A.movement_uid,
		CASE WHEN B.interpolated = TRUE THEN SUM(A.volume)*15.0/((EXTRACT(minutes FROM B.end_time - B.start_time)+1)*1.0) ELSE SUM(A.volume) END AS volume
	FROM rliu.volumes A
	INNER JOIN bins B USING (intersection_uid)
	WHERE A.datetime_bin BETWEEN B.start_time AND B.end_time
	GROUP BY A.intersection_uid, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900), A.classification_uid, A.leg, A.movement_uid, B.interpolated, (EXTRACT(minutes FROM B.end_time - B.start_time)+1)

	RETURNING volume_15min_tmc_uid, intersection_uid, datetime_bin, classification_uid, leg, movement_uid
	)
	UPDATE rliu.volumes a
	SET volume_15min_tmc_uid = b.volume_15min_tmc_uid
	FROM aggregate_insert b
	WHERE a.datetime_bin >= b.datetime_bin AND a.datetime_bin < b.datetime_bin + INTERVAL '15 minutes'
	AND a.classification_uid  = b.classification_uid 
	AND a.leg = b.leg
	AND a.movement_uid = b.movement_uid
	AND a.intersection_uid = b.intersection_uid
	AND a.volume_15min_tmc_uid IS NULL;
	RAISE NOTICE '% Inserted and Updated Primary Key', timeofday();
    RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION aggregate_15_min_tmc3()
  OWNER TO rliu;
GRANT EXECUTE ON FUNCTION aggregate_15_min_tmc3() TO public;
GRANT EXECUTE ON FUNCTION aggregate_15_min_tmc3() TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION aggregate_15_min_tmc3() TO bdit_humans WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION aggregate_15_min_tmc3() TO rliu;

