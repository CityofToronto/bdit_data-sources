CREATE VIEW open_data.bluetooth_segments AS 
	SELECT segment_name, analysis_id, street, direction, start_crossstreet, end_street, end_crossstreet, start_date, end_date, length, bluetooth, wifi,  geom
	
	FROM bluetooth.segments
	INNER JOIN bluetooth.report_active_dates USING (analysis_id)
	WHERE NOT duplicate