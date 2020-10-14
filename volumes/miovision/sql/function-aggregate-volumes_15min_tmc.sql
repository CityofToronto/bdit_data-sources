CREATE OR REPLACE FUNCTION miovision_csv.aggregate_15_min_tmc_2020(
	start_date date,
	end_date date)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

BEGIN

WITH zero_padding_movements AS (
		/*Cross product of legal movement for cars, bikes, and peds and the bins to aggregate*/
		SELECT m.*, datetime_bin15 
		FROM miovision_api.intersection_movements m
		CROSS JOIN generate_series(start_date - interval '1 hour', end_date - interval '1 hour 15 minutes', INTERVAL '15 minutes') AS dt(datetime_bin15)
		-- make sure that the intersection is still active
		WHERE intersection_uid IN (SELECT intersection_uid FROM miovision_api.intersections 
				WHERE start_date::date >= date_installed 
				AND date_decommissioned IS NULL)
		)
, aggregate_insert AS (
INSERT INTO miovision_csv.volumes2020_15min_tmc(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)

SELECT 
pad.intersection_uid,
pad.datetime_bin15 AS datetime_bin,
pad.classification_uid,
pad.leg,
pad.movement_uid,
CASE WHEN un.accept = FALSE THEN NULL ELSE (COALESCE(SUM(A.volume), 0)) END AS volume
FROM zero_padding_movements pad
--To set unacceptable ones to NULL instead (& only gap fill light vehicles, cyclist and pedestrian)
LEFT JOIN miovision_csv.unacceptable_gaps_2020 un 
	ON un.intersection_uid = pad.intersection_uid
	AND pad.datetime_bin15 >= DATE_TRUNC('hour', gap_start)
	AND pad.datetime_bin15 < DATE_TRUNC('hour', gap_end) + interval '1 hour' -- may get back to this later on for fear of removing too much data
--To get 1min bins
LEFT JOIN miovision_csv.volumes_2020 A
	ON A.datetime_bin >= start_date - INTERVAL '1 hour' 
	AND A.datetime_bin < end_date - INTERVAL '1 hour'
	AND A.datetime_bin >= pad.datetime_bin15 
	AND A.datetime_bin < pad.datetime_bin15 + interval '15 minutes'
	AND A.intersection_uid = pad.intersection_uid 
	AND A.classification_uid = pad.classification_uid
	AND A.leg = pad.leg 
	AND A.movement_uid = pad.movement_uid
WHERE A.volume_15min_tmc_uid IS NULL
GROUP BY pad.intersection_uid, pad.datetime_bin15, pad.classification_uid, pad.leg, pad.movement_uid, un.accept
HAVING pad.classification_uid IN (1,2,6) OR SUM(A.volume) > 0 
RETURNING intersection_uid, volume_15min_tmc_uid, datetime_bin, classification_uid, leg, movement_uid, volume
)

--To update foreign key for 1min bin table
UPDATE miovision_csv.volumes_2020 a
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
	
RAISE NOTICE '% Done aggregating to 15min TMC bin', timeofday();
END;

$BODY$;

ALTER FUNCTION miovision_csv.aggregate_15_min_tmc_2020(date, date)
    OWNER TO czhu;

GRANT EXECUTE ON FUNCTION miovision_csv.aggregate_15_min_tmc_2020(date, date) TO PUBLIC;

GRANT EXECUTE ON FUNCTION miovision_csv.aggregate_15_min_tmc_2020(date, date) TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_csv.aggregate_15_min_tmc_2020(date, date) TO czhu;

