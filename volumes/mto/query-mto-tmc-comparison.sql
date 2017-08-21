-- Called from Python for a side-by-side comparison of MTO loops and turning movement counts on select high way ramps
-- Parameters: $1 - detector_id, $2 - count_date_mto, $3 - centreline_id, $4 - count_date_tmc

SELECT LEFT(hwy_name,3), loc_desc, time_15, C.volume AS mto_vol, D.volume AS tmc_vol
FROM (SELECT hwy_name, loc_desc, UNNEST(ARRAY[time_15, time_15+1]) AS time_15,
		(CASE WHEN volume0 IS NULL AND volume2 IS NULL THEN UNNEST(ARRAY[volume1/2, volume1/2])
		WHEN volume0 IS NULL THEN UNNEST(ARRAY[(volume1*volume1/(volume1+volume2)), (volume2*volume1/(volume1+volume2))]) 
		WHEN volume2 IS NULL THEN UNNEST(ARRAY[volume0*volume1/(volume0+volume1), volume1*volume1/(volume0+volume1)]) 
		ELSE UNNEST(ARRAY[(volume1-(volume2-volume0)/6)/2, (volume1+(volume2-volume0)/6)/2]) END) AS volume 
	FROM (SELECT row_number() OVER w, (CASE WHEN time_15 - (LAG(time_15) OVER w) = 2 THEN LAG(volume) OVER w ELSE NULL END) AS volume0, 
				volume AS volume1, 
				(CASE WHEN (LEAD(time_15) OVER w) - time_15 = 2 THEN LEAD(volume) OVER w ELSE NULL END) AS volume2, time_15, loc_desc, hwy_name
		FROM (SELECT detector_id, count_bin::date AS count_date, EXTRACT(HOUR from count_bin)*4 + extract(minute from count_bin)::int/15 AS time_15, volume, loc_desc, hwy_name
				FROM mto.mto_agg_30 JOIN mto.sensors USING (detector_id)
				WHERE detector_id = $1 and count_bin::date= $2 AND volume != 0) A
		WINDOW w AS (PARTITION BY detector_id, count_date ORDER BY count_date)) B) C 
	
	FULL OUTER JOIN 
	
	(select EXTRACT(HOUR from count_bin)*4 + extract(minute from count_bin)::int/15 AS time_15, volume
	from prj_volume.centreline_volumes 
	where count_bin::date = $4 and centreline_id = $3
	order by count_bin) D

	USING (time_15)
ORDER BY time_15
