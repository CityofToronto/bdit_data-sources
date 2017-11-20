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
	SELECT * FROM bluetooth.raw_data rs;

	-- LOAD bluetooth.aggr_5min with new data
	INSERT INTO bluetooth.aggr_5min (analysis_id, datetime_bin, tt, obs)
	SELECT	rs.analysis_id,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' +
		INTERVAL '1 second' * (floor((extract('epoch' from rs.measured_timestamp)-1) / 300) * 300) as datetime_bin,
		median(rs.measured_time) AS travel_time,
		COUNT(rs.user_id) AS obs
	FROM bluetooth.raw_data rs
	WHERE rs.outlier_level = 0 AND device_class = 1
	GROUP BY rs.analysis_id, (floor((extract('epoch' from rs.measured_timestamp)-1) / 300) * 300)
	ORDER BY rs.analysis_id, (floor((extract('epoch' from rs.measured_timestamp)-1) / 300) * 300);
	RETURN 1;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
   GRANT EXECUTE ON FUNCTION bluetooth.move_raw_data() TO aharpal;
   GRANT EXECUTE ON FUNCTION bluetooth.move_raw_data() TO bt_insert_bot;