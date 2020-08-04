CREATE OR REPLACE FUNCTION miovision_api.aggregate_15_min_tmc(
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
		FROM miovision_api.intersection_movements_new m
		CROSS JOIN generate_series(start_date - interval '1 hour', end_date - interval '1 hour 15 minutes', INTERVAL '15 minutes') AS dt(datetime_bin15)
		WHERE classification_uid IN (1,2,6) 
		)
, aggregate_insert AS (
INSERT INTO miovision_api.volumes_15min_tmc(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)

SELECT 
pad.intersection_uid,
pad.datetime_bin15,
pad.classification_uid,
pad.leg,
pad.movement_uid,
CASE WHEN un.accept = FALSE THEN NULL ELSE (COALESCE(SUM(A.volume), 0)) END AS volume
FROM zero_padding_movements pad
--To set unacceptable ones to NULL instead (& only gap fill  light vehicles, cyclist and pedestrian)
LEFT JOIN miovision_api.unacceptable_gaps un
	ON un.intersection_uid = pad.intersection_uid
	AND DATE_TRUNC('hour', un.gap_start) = DATE_TRUNC('hour', pad.datetime_bin15) 	
--To get 1min bins
FULL OUTER JOIN miovision_api.volumes A
	ON datetime_bin >= start_date - INTERVAL '1 hour' 
	AND datetime_bin < end_date - INTERVAL '1 hour'
	AND A.intersection_uid = pad.intersection_uid 
	AND A.classification_uid = pad.classification_uid
	AND A.leg = pad.leg 
	AND A.movement_uid = pad.movement_uid
	AND A.datetime_bin >= pad.datetime_bin15 
	AND A.datetime_bin < pad.datetime_bin15 + interval '15 minutes'
-- make sure that the intersection is still active
WHERE pad.intersection_uid IN (SELECT intersection_uid FROM miovision_api.intersections_new 
				WHERE start_date::date > date_installed 
				AND date_decommissioned IS NULL)
AND A.volume_15min_tmc_uid IS NULL
GROUP BY pad.intersection_uid, pad.datetime_bin15, pad.classification_uid,pad.leg,pad.movement_uid, un.accept
RETURNING intersection_uid, volume_15min_tmc_uid, datetime_bin, classification_uid, leg, movement_uid, volume
)

--To update foreign key for 1min bin table
UPDATE miovision_api.volumes a
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
