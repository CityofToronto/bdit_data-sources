DELETE FROM bluetooth.raw_store
WHERE startpoint_name = 'B' AND endpoint_name = 'A' AND measured_timestamp > '2014-03-01 00:00:00' AND measured_timestamp <= '2014-04-01 00:00:00'