DROP VIEW IF EXISTS open_data.ksp_travel_times_2017;
CREATE OR REPLACE VIEW open_data.ksp_travel_times_2017 AS 
SELECT 
	segment_name AS result_id, 	
		B.datetime_bin,
		c.day_type, 
		category,
		period_name,
		tt,
		obs
                
	FROM king_pilot.bt_segments A
	INNER JOIN bluetooth.aggr_5min B USING (analysis_id)
	JOIN king_pilot.date_lookup c ON dt = datetime_bin::DATE
	LEFT OUTER JOIN king_pilot.periods d ON c.day_type = d.day_type AND datetime_bin::TIME <@ d.period_range
	JOIN bluetooth.segments USING (analysis_id)
	WHERE NOT( A.bt_id = 33  AND B.datetime_bin::date = '2017-09-19' AND B.datetime_bin::time >= '19:00') AND B.datetime_bin < '2018-01-01';
GRANT ALL ON open_data.ksp_travel_times_2017 TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON open_data.ksp_travel_times_2017 TO dbadmin;
 GRANT SELECT ON open_data.ksp_travel_times_2017 TO bdit_humans;

DROP VIEW IF EXISTS open_data.ksp_travel_times_2018 CASCADE;
CREATE OR REPLACE VIEW open_data.ksp_travel_times_2018 AS 
SELECT 
	segment_name AS result_id, 	
		B.datetime_bin,
		c.day_type, 
		category,
		period_name,
		tt,
		obs
                
                
	FROM king_pilot.bt_segments A
	INNER JOIN bluetooth.aggr_5min B USING (analysis_id)
	JOIN king_pilot.date_lookup c ON dt <= datetime_bin AND dt + INTERVAL '1 day' > datetime_bin
	LEFT OUTER JOIN king_pilot.periods d ON c.day_type = d.day_type AND datetime_bin::TIME <@ d.period_range
	JOIN bluetooth.segments USING (analysis_id)
	WHERE datetime_bin >= '2018-01-01' AND B.datetime_bin < LEAST(date_trunc('month', current_date) - INTERVAL '1 Month', '2018-01-01'::DATE + INTERVAL '1 year');
GRANT ALL ON open_data.ksp_travel_times_2018 TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON open_data.ksp_travel_times_2018 TO dbadmin;
 GRANT SELECT ON open_data.ksp_travel_times_2018 TO bdit_humans;
