CREATE OR REPLACE FUNCTION mohan.route_point_status(IN insert_value date)
    RETURNS TABLE(
	route_pnt int, 
	reader text,
	active_date date, 
	active boolean
	)
    LANGUAGE 'plpgsql'
AS 
$$	
	begin
		return query
			with x AS(
SELECT analysis_id, start_route_point_id AS route_point, start_detector as detector
	FROM mohan.bt_lookup
	UNION
	SELECT analysis_id, end_route_point_id AS route_point, end_detector as detector
	FROM mohan.bt_lookup
	ORDER by detector
),
	y as (
		select 
	x.route_point, x.detector, MAX (datetime_bin::date) as dt,
			CASE
			WHEN  MAX (datetime_bin::date)>= (insert_value - 1) then 'true'::bool
		else 'false'::boolean
		END 
		AS active
		from 
		bluetooth.all_analyses
		LEFT JOIN bluetooth.aggr_5min USING (analysis_id)
		LEFT JOIN x USING (analysis_id)
		where  bluetooth.all_analyses.pull_data = 'true'
		GROUP BY x.route_point, x.detector
	)
	,a_detector as (
		SELECT distinct(detector), max(dt) as latest_date
		From y
		GROUP BY detector
	), 
	latest as(
SELECT route_point, detector, dt, latest_date
		from y
		LEFT JOIN a_detector using (detector)
	)
	, final AS (
		Select route_point, detector, 
		CASE
			WHEN  latest_date::text::date > dt OR latest_date::text::date > (insert_value) then (insert_value)
			else dt
			END
			AS last_active_date
		from latest
		ORDER BY detector
		)
	SELECT route_point, detector, last_active_date as dt, 
	CASE
	WHEN last_active_date >=(insert_value - 1) then 'true'::bool
		else 'false'::boolean
		END 
		AS active
		from final;
		end; $$;