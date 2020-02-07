CREATE OR REPLACE VIEW cycling.cycling_perm_old AS
SELECT 	A.centreline_id,
	A.dir AS direction,
	A.location,
	'Cyclists' AS class_type,
	B.datetime_start + (C.lag * interval '1 minute') AS datetime_bin,
	B.volume * CASE WHEN C.lag = 0 THEN 1 ELSE 0 END AS volume_15min
FROM cycling.count_perm_station A
INNER JOIN cycling.count_perm_data B USING (station_id)
CROSS JOIN (SELECT unnest AS lag FROM unnest(ARRAY[0,15,30,45])) C
ORDER BY A.centreline_id, A.dir, B.datetime_start + (C.lag * interval '1 minute')