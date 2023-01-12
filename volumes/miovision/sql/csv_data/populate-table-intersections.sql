TRUNCATE miovision.intersections;

INSERT INTO miovision.intersections(intersection_name, lat, lng)
SELECT 	TRIM(substring(replace(study_name,'Adelaide/','Adelaide /') FROM '(?:(?!Oct|Nov|Dec).)*')) AS intersection_name,
	lat,
	lng
FROM 	miovision.raw_data
GROUP BY lat, lng, TRIM(substring(replace(study_name,'Adelaide/','Adelaide /') FROM '(?:(?!Oct|Nov|Dec).)*')), substring(study_name FROM '([a-zA-Z]*)')
ORDER BY substring(study_name FROM '([a-zA-Z]*)'), lat;

UPDATE miovision.intersections
SET street_main = TRIM(substring(intersection_name FROM '([a-zA-Z]*)'));

UPDATE miovision.intersections
SET street_cross = TRIM(substring(intersection_name FROM '\/ (.*)'));