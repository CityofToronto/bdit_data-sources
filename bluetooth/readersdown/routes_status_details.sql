CREATE OR REPLACE FUNCTION mohan.route_status_details(IN insert_value date)
    RETURNS TABLE(
	an_id bigint, 
	last_received date, 
	status text, 
	ddown integer, 
	start_route_point_id text, 
	start_detector text, 
	end_route_point_id text, 
	end_detector text
	)
    LANGUAGE 'plpgsql'
AS 
$$	
	begin
		return query
			with x AS 
			(select DISTINCT(analysis_id), MAX (datetime_bin::date),
			CASE
			WHEN  MAX (datetime_bin::date)>= (insert_value-1) then 'active'::text
		else 'inactive'::text
		END 
		AS route_status,
		CASE
		WHEN MAX (datetime_bin::date) >= (insert_value-1) then 0
		else (insert_value - MAX (datetime_bin::date))
		END AS days_down
		from 
		bluetooth.all_analyses
		LEFT JOIN bluetooth.aggr_5min USING (analysis_id) 
		
		--where
		--datetime_bin = insert_value
		GROUP BY analysis_id)
		, y AS (
		 SELECT all_analyses.analysis_id,
    (all_analyses.route_points -> 0) ->> 'id'::text AS start_route_point_id,
    (all_analyses.route_points -> 0) ->> 'name'::text AS start_detector,
    (all_analyses.route_points -> 1) ->> 'id'::text AS end_route_point_id,
    (all_analyses.route_points -> 1) ->> 'name'::text AS end_detector
   FROM bluetooth.all_analyses)
   , z AS (
   		SELECT *
		from x
		LEFT JOIN y USING (analysis_id))
		SELECT * 
		from z;
		end; $$;