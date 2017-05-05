INSERT INTO bluetooth.aggr_5min (analysis_id, datetime_bin, tt, obs)
SELECT
	B.analysis_id,
	A.measured_bin - INTERVAL '5 minutes' AS datetime_bin,
	A.medianmeasuredtime AS tt,
	A.samplecount AS obs

	FROM bluetooth.dl_data A

	INNER JOIN bluetooth.ref_segments B ON A.startpoint_name = B.orig_startpointname AND A.endpoint_name = B.orig_endpointname

	WHERE A.samplecount > 0 -- AND measured_bin > '2017-04-24'