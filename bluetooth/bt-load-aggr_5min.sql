TRUNCATE bluetooth.aggr_5min;

INSERT INTO bluetooth.aggr_5min (analysis_id, datetime_bin, tt, obs)
SELECT
	OBS.analysis_id,
	TIMESTAMP WITHOUT TIME ZONE 'epoch' +
		INTERVAL '1 second' * (floor((extract('epoch' from OBS.measured_timestamp)-1) / 300) * 300) as datetime_bin,
	median(OBS.measured_time) AS travel_time,
	COUNT(OBS.id) AS obs


	FROM bluetooth.observations OBS
	WHERE OBS.outlier_level = 0 AND device_class = 1
	GROUP BY OBS.analysis_id, (floor((extract('epoch' from OBS.measured_timestamp)-1) / 300) * 300)
	ORDER BY OBS.analysis_id, (floor((extract('epoch' from OBS.measured_timestamp)-1) / 300) * 300);