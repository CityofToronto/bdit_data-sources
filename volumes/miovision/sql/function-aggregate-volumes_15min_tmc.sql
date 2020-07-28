CREATE OR REPLACE FUNCTION miovision_api.aggregate_15_min_tmc(
	start_date date,
	end_date date)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

BEGIN

INSERT INTO miovision_api.volumes_15min_tmc(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)

SELECT 
m.intersection_uid,
dt.datetime_bin,
m.classification_uid,
m.leg,
m.movement_uid,
CASE WHEN un.accept = FALSE THEN NULL ELSE (COALESCE(SUM(A.volume), 0)) END AS volume
FROM miovision_api.intersection_movements_new m
--Cross product of legal movement for cars, bikes, and peds and the bins to aggregate
CROSS JOIN generate_series(start_date - interval '1 hour', end_date - interval '1 hour 15 minutes', INTERVAL '15 minutes') AS dt(datetime_bin)
--To get 1min bins
LEFT JOIN miovision_api.volumes A
	ON A.intersection_uid = m.intersection_uid AND A.classification_uid = m.classification_uid
	AND A.leg = m.leg AND A.movement_uid = m.movement_uid
	AND dt.datetime_bin = datetime_bin_15(A.datetime_bin)
--To set unacceptable ones to NULL instead
LEFT JOIN miovision_api.unacceptable_gaps un
	ON un.intersection_uid = A.intersection_uid
	AND DATE_TRUNC('hour', un.gap_start) = DATE_TRUNC('hour', dt.datetime_bin) 	
--Only interested in light vehicles, cyclist and pedestrian
WHERE m.classification_uid IN (1,2,6) 
-- make sure that the intersection is still active
AND m.intersection_uid IN (SELECT intersection_uid FROM miovision_api.intersections_new 
				WHERE start_date::date > date_installed 
				AND date_decommissioned IS NULL)
AND A.volume_15min_tmc_uid IS NULL
GROUP BY m.intersection_uid, dt.datetime_bin, m.classification_uid,m.leg,m.movement_uid, un.accept;

--To update foreign key for 1min bin table
UPDATE miovision_api.volumes a
SET volume_15min_tmc_uid = b.volume_15min_tmc_uid
FROM miovision_api.volumes_15min_tmc b
WHERE a.datetime_bin >= start_date - interval '1 hour' 
AND a.datetime_bin < end_date -  interval '1 hour'
AND a.volume_15min_tmc_uid IS NULL AND b.volume > 0 
AND a.intersection_uid  = b.intersection_uid 
AND a.datetime_bin >= b.datetime_bin 
AND a.datetime_bin < b.datetime_bin + INTERVAL '15 minutes'
AND a.classification_uid  = b.classification_uid 
AND a.leg = b.leg
AND a.movement_uid = b.movement_uid
;
	
RAISE NOTICE '% Done aggregating to 15min TMC bin', timeofday();
END;

$BODY$;

ALTER FUNCTION miovision_api.aggregate_15_min_tmc(date, date)
    OWNER TO jchew;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_tmc(date, date) TO PUBLIC;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_tmc(date, date) TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min_tmc(date, date) TO jchew;

