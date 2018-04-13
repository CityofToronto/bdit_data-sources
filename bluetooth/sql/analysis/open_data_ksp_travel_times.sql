CREATE OR REPLACE VIEW open_data.ksp_travel_times AS 
SELECT 
	segment_name, 	
		B.datetime_bin,
		day_type, 
		category,
		tt,
		obs,
                period_name
                
	FROM king_pilot.bt_segments A
	INNER JOIN bluetooth.aggr_5min B USING (analysis_id)
	JOIN king_pilot.date_lookup c ON dt = datetime_bin
	JOIN king_pilot.periods d USING (day_type)
	JOIN bluetooth.segments USING (analysis_id)
	WHERE NOT A.bt_id = 33 AND B.datetime_bin >= '2017-09-19 19:00';
GRANT ALL ON open_data.ksp_travel_times TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON open_data.ksp_travel_times TO dbadmin;
 GRANT SELECT ON open_data.ksp_travel_times TO bdit_humans;
