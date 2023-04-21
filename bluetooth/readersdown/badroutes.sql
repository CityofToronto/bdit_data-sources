SELECT segments.analysis_id
FROM bluetooth.segments
EXCEPT
SELECT DISTINCT f.analysis_id
FROM (SELECT
    observations.analysis_id,
    observations.measured_timestamp
FROM bluetooth.observations
WHERE
    observations.measured_timestamp <= ((
        SELECT max(observations_1.measured_timestamp) AS max
        FROM bluetooth.observations AS observations_1
    ))
    AND observations.measured_timestamp >= ((
        SELECT max(observations_1.measured_timestamp) - '02:00:00'::interval
        FROM bluetooth.observations AS observations_1
    ))
    AND (observations.analysis_id IN (
        SELECT segments.analysis_id
        FROM bluetooth.segments
    ))
ORDER BY observations.measured_timestamp DESC) AS f