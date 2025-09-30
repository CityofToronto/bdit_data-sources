-- Drop temp table if it exists
DROP TABLE IF EXISTS temp_min_dates;

-- Recreate temp table with restriction to specific intersection_uids
CREATE TEMP TABLE temp_min_dates AS
SELECT
    intersection_uid,
    MIN(datetime_bin)::date AS min_datetime
FROM miovision_api.volumes
WHERE intersection_uid IN (4440, 4441, 4442, 4443, 4444, 4445, 4446, 4447, 4439)
GROUP BY intersection_uid;

-- Create index
CREATE INDEX idx_temp_intersection_uid ON temp_min_dates (intersection_uid);

-- Perform the update
UPDATE miovision_api.intersections AS i
SET date_installed = t.min_datetime
FROM temp_min_dates AS t
WHERE i.intersection_uid = t.intersection_uid;
