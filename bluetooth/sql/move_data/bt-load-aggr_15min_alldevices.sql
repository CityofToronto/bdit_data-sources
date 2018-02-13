TRUNCATE bluetooth.aggr_15min_alldevices;

INSERT INTO bluetooth.aggr_15min_alldevices (analysis_id, datetime_bin, tt, obs)
SELECT
	A.analysis_id,
	TIMESTAMP WITHOUT TIME ZONE 'epoch' +
		INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) as datetime_bin,
	AVG(A.tt) AS tt,
	SUM(A.obs) AS obs


	FROM bluetooth.aggr_5min_alldevices A
	GROUP BY A.analysis_id, (floor((extract('epoch' from A.datetime_bin)) / 900) * 900)
	ORDER BY A.analysis_id, (floor((extract('epoch' from A.datetime_bin)) / 900) * 900);