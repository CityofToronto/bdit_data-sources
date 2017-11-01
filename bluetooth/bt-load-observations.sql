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
  -- WHERE rs.measured_timestamp > '2017-04-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2017-05-01 00:00:00'::timestamp without time zone;