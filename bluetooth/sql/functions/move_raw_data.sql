CREATE OR REPLACE FUNCTION bluetooth.move_raw_data()
  RETURNS integer AS
$BODY$
BEGIN
	-- TRANSFER bluetooth.raw_data TO bluetooth.observations
	INSERT INTO bluetooth.observations (user_id,analysis_id,
	  measured_time,
	  measured_time_no_filter,
	  startpoint_number,
	  startpoint_name,
	  endpoint_number,
	  endpoint_name,
	  measured_timestamp,
	  outlier_level,
	  cod,
	  device_class)
	SELECT * FROM bluetooth.raw_data rs
	ON CONFLICT DO NOTHING; --If there are duplicate rows don't insert them

	-- LOAD bluetooth.aggr_5min with new data
	INSERT INTO bluetooth.aggr_5min (analysis_id, datetime_bin, tt, obs)
	SELECT	rs.analysis_id,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' +
		INTERVAL '1 second' * (floor((extract('epoch' from rs.measured_timestamp)-1) / 300) * 300) as datetime_bin,
		percentile_cont(0.5) WITHIN GROUP (ORDER BY rs.measured_time) AS travel_time,
		COUNT(rs.user_id) AS obs
	FROM bluetooth.raw_data rs
	JOIN bluetooth.class_of_device USING (cod)
	LEFT OUTER JOIN bluetooth.segments USING (analysis_id)
	WHERE rs.outlier_level = 0 AND (
					( street LIKE 'Gardiner%' OR street LIKE 'Lakeshore%' OR street LIKE 'DVP%') --Allow all device types on the highways
		OR 		(--Restrict Arterial device types
					device_class = 1 AND class_of_device.device_type = ANY (ARRAY['Hands-free Device', 'WiFi']) ))
	GROUP BY rs.analysis_id, datetime_bin
	ON CONFLICT DO NOTHING;
	RETURN 1;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  SECURITY DEFINER
  COST 100;
  ALTER FUNCTION bluetooth.move_raw_data()
  OWNER TO bt_admins;
   GRANT EXECUTE ON FUNCTION bluetooth.move_raw_data() TO aharpal;
   GRANT EXECUTE ON FUNCTION bluetooth.move_raw_data() TO bt_insert_bot;
GRANT EXECUTE ON FUNCTION bluetooth.move_raw_data() TO bt_admins;
