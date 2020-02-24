CREATE MATERIALIZED VIEW open_data.wys_stationary_locations AS

SELECT area_short::INT as ward_no, sign_id, address, sign_name, dir, schedule, 
    min_speed, 
    speed_limit, 
    flash_speed, 
    strobe_speed, 
	CASE WHEN prev_start IS NOT NULL THEN start_date ELSE MIN(datetime_bin) END AS start_date,
	LEAST(next_start, MAX(datetime_bin)) AS end_date,
	geom
	FROM wys.stationary_signs
	JOIN wys.sign_schedules_list USING (api_id)
     JOIN wys.sign_schedules_clean USING (schedule_name)
	 JOIN wys.speed_counts_agg USING (api_id) 
     LEFT JOIN gis.wards2018 ON ST_Contains(wkb_geometry, geom)
	 GROUP BY ward, sign_id, address, sign_name, dir, schedule, 
            min_speed, 
            speed_limit, prev_start,next_start, start_date,
            flash_speed, 
            strobe_speed, geom