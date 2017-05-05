SELECT user_id, analysis_id, measured_time, measured_timestamp
FROM bluetooth.observations
GROUP BY user_id, analysis_id, measured_time, measured_timestamp
HAVING COUNT(*) > 1
