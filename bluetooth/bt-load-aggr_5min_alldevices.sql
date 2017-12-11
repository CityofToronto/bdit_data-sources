TRUNCATE bluetooth.aggr_5min_alldevices;

INSERT INTO bluetooth.aggr_5min_alldevices (analysis_id, datetime_bin, tt, obs)
SELECT	rs.analysis_id,
	TIMESTAMP WITHOUT TIME ZONE 'epoch' +
	INTERVAL '1 second' * (floor((extract('epoch' from rs.measured_timestamp)-1) / 300) * 300) as datetime_bin,
	percentile_cont(0.5) WITHIN GROUP (ORDER BY rs.measured_time) AS travel_time,
	COUNT(rs.user_id) AS obs
FROM bluetooth.observations rs
WHERE rs.outlier_level = 0
GROUP BY rs.analysis_id, (floor((extract('epoch' from rs.measured_timestamp)-1) / 300) * 300)
ORDER BY rs.analysis_id, (floor((extract('epoch' from rs.measured_timestamp)-1) / 300) * 300);