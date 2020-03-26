INSERT INTO rescu.volumes_15min (detector_id, datetime_bin, volume_15min, arterycode)

WITH raw_data AS (	
	SELECT 	TRIM(SUBSTRING(a.raw_info, 15, 12)) AS detector_id,
			dt + LEFT(a.raw_info,5)::time AS datetime_bin,
			nullif(TRIM(SUBSTRING(a.raw_info, 27, 10)),'')::int AS volume_15min
	FROM rescu.raw_15min a
)

SELECT 	detector_id,
		a.datetime_bin,
		a.volume_15min,
		b.arterycode
FROM	raw_data a
LEFT JOIN rescu.detector_inventory b USING (detector_id)
WHERE a.volume_15min >= 0
ORDER BY datetime_bin, detector_id