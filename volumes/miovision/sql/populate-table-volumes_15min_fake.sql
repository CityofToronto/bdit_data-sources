DROP TABLE IF EXISTS miovision.volumes_15min_fake;
DROP TABLE IF EXISTS staging;
DROP TABLE IF EXISTS time_pct;

CREATE TEMPORARY TABLE staging (
	intersection_uid integer,
	dir text, 
	leg text,
	datetime_bin timestamp without time zone,
	tm time without time zone,
	day_type text,
	scale_pct numeric,
	scale_volume numeric,
	average_pct numeric,
	volume numeric
);

CREATE TEMPORARY TABLE time_pct (
	intersection_uid integer,
	dir text,
	leg text,
	day_type text,
	tm time without time zone,
	average_pct numeric
);

CREATE TABLE miovision.volumes_15min_fake (
	volume_15min_uid serial not null,
	intersection_uid integer,
	datetime_bin timestamp without time zone,
	classification_uid integer,
	leg text,
	dir text,
	volume numeric
);

WITH 	complete_days AS (
		SELECT intersection_uid, dir, leg, datetime_bin::date AS dt, SUM(volume) AS daily_volume FROM miovision.volumes_15min WHERE classification_uid = 1 GROUP BY intersection_uid, dir, leg, datetime_bin::date HAVING COUNT(DISTINCT datetime_bin) = 96
	),
	daily_pct AS (
		SELECT	intersection_uid,
			dir,
			leg,
			datetime_bin::date as dt,
			datetime_bin::time AS tm,	
			CASE WHEN EXTRACT(isodow FROM datetime_bin) <= 5 THEN 'Weekday' ELSE 'Weekend' END AS day_type,
			CASE WHEN B.daily_volume = 0 THEN 0 ELSE A.volume/B.daily_volume*1.0 END as pct
		FROM miovision.volumes_15min A
		INNER JOIN complete_days B USING (intersection_uid, dir, leg)
		WHERE A.datetime_bin::date = B.dt AND classification_uid = 1
	)
	
INSERT INTO time_pct
SELECT intersection_uid, dir, leg, day_type, tm, AVG(pct) AS average_pct
FROM daily_pct
GROUP BY intersection_uid, dir, leg, day_type, tm
ORDER BY intersection_uid, dir, leg, day_type, tm;

WITH
	daily_scale AS (
		SELECT	intersection_uid,
			dir,
			leg,
			A.datetime_bin::date as dt,
			SUM(B.average_pct) AS scale_pct,
			SUM(A.volume) AS scale_volume
		FROM miovision.volumes_15min A
		INNER JOIN time_pct B USING (intersection_uid, dir, leg)
		WHERE A.datetime_bin::time = B.tm AND CASE WHEN EXTRACT(isodow FROM A.datetime_bin) <= 5 THEN 'Weekday' ELSE 'Weekend' END = B.day_type AND A.classification_uid = 1
		GROUP BY intersection_uid, dir, leg, A.datetime_bin::date
	),
	all_data AS (
		SELECT intersection_uid, dir, leg, datetime_bin
		FROM miovision.volumes_15min
		WHERE classification_uid = 1
	),
	skeleton AS (
		WITH 
			tms AS (SELECT dt_tm::date as dt, dt_tm AS datetime_bin FROM generate_series('2017-09-01 00:00'::timestamp,'2018-04-30 23:45'::timestamp,'15 minutes'::interval) dt_tm)
		SELECT A.intersection_uid, A.dir, A.leg, B.datetime_bin, B.datetime_bin::date as dt, B.datetime_bin::time as tm, CASE WHEN EXTRACT(isodow FROM B.datetime_bin) <= 5 THEN 'Weekday' ELSE 'Weekend' END AS day_type
		FROM daily_scale A
		INNER JOIN tms B USING (dt)
		LEFT JOIN all_data C USING (intersection_uid, dir, leg, datetime_bin)
		WHERE C.dir IS NULL
	)

INSERT INTO staging(intersection_uid, dir, leg, datetime_bin, tm, day_type, scale_pct, scale_volume)
SELECT A.intersection_uid, A.dir, A.leg, A.datetime_bin, A.tm, A.day_type, B.scale_pct, B.scale_volume
FROM skeleton A
INNER JOIN daily_scale B USING (intersection_uid, dir, leg, dt)
ORDER BY A.intersection_uid, A.dir, A.leg, A.datetime_bin;


CREATE INDEX vol_ind ON staging(intersection_uid, dir, leg, day_type, tm);
CREATE INDEX time_pct_ind ON time_pct(intersection_uid, dir, leg, day_type, tm);

UPDATE staging A
SET average_pct = B.average_pct
FROM time_pct B 
WHERE 	A.intersection_uid = B.intersection_uid AND
	A.dir = B.dir AND
	A.leg = B.leg AND
	A.day_type = B.day_type AND
	A.tm = B.tm;


UPDATE staging
SET volume = CASE WHEN scale_pct = 0 THEN 0 ELSE average_pct/scale_pct*scale_volume*1.0 END;


INSERT INTO miovision.volumes_15min_fake(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
SELECT intersection_uid, datetime_bin, 1 as classification_uid, leg, dir, volume
FROM staging
ORDER BY intersection_uid, datetime_bin, leg, dir;
