DROP MATERIALIZED VIEW open_data.wys_stationary_locations CASCADE;
CREATE MATERIALIZED VIEW open_data.wys_stationary_locations AS

SELECT area_short::INT as ward_no, sign_id, address, sign_name, dir, schedule, 
    min_speed, 
    speed_limit, 
    flash_speed, 
    strobe_speed, 
	CASE WHEN prev_start IS NOT NULL THEN start_date ELSE MIN(datetime_bin) END AS start_date,
	CASE WHEN max(datetime_bin) < date_trunc('month'::text, now())
		 THEN LEAST(next_start::timestamp without time zone, 
		            max(datetime_bin))
		END AS end_date,
	geom
	FROM wys.stationary_signs
	JOIN wys.sign_schedules_list USING (api_id)
     JOIN wys.sign_schedules_clean USING (schedule_name)
	 JOIN wys.speed_counts_agg_5kph USING (api_id) 
     LEFT JOIN gis.wards2018 ON ST_Contains(wkb_geometry, geom)
	 GROUP BY ward_no, sign_id, address, sign_name, dir, schedule, 
            min_speed, 
            speed_limit, prev_start,next_start, start_date,
            flash_speed, 
            strobe_speed, geom;
CREATE UNIQUE INDEX ON open_data.wys_stationary_locations (sign_id);

CREATE FUNCTION wys.refresh_od_mat_view() 
RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY open_data.wys_stationary_locations WITH DATA ;
$BODY$;
REVOKE EXECUTE ON FUNCTION wys.refresh_od_mat_view()FROM public;
GRANT EXECUTE ON FUNCTION wys.refresh_od_mat_view()TO wys_bot;