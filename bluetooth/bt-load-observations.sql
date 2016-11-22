INSERT INTO bluetooth.observations_201402 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-02-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-03-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201403 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-03-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-04-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201404 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-04-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-05-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201405 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-05-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-06-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201406 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-06-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-07-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201407 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-07-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-08-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201408 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-08-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-09-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201409 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-09-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-10-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201410 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-10-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-11-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201411 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-11-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2014-12-01 00:00:00'::timestamp without time zone;

INSERT INTO bluetooth.observations_201412 (user_id,analysis_id,
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
  SELECT * FROM bluetooth.raw_store rs
  WHERE rs.measured_timestamp > '2014-12-01 00:00:00'::timestamp without time zone AND rs.measured_timestamp <= '2015-01-01 00:00:00'::timestamp without time zone;