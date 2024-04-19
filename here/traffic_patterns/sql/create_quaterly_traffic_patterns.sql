CREATE TABLE here.traffic_pattern_yy_v_b_yyyyqq AS 
-- Aggregate link_dir based daily hourly speed
WITH daily_time_cost as (
	SELECT
		routing_streets.link_dir,
		dt, 
		tod, 
		harmean(ta.pct_50) AS daily_cost
	FROM here.routing_streets_yy_q AS routing_streets
	LEFT JOIN here.ta USING (link_dir)
	LEFT JOIN ref.holiday USING (dt)
	WHERE
		dt >= _start_date AND dt < _end_date AND --
		extract(isodow from dt) in (2,3,4) AND
		holiday.dt IS null
	GROUP BY routing_streets.link_dir, dt, tod
)


SELECT 
	link_dir, 
 	tod,
	harmean(daily_cost)::INT AS cost
FROM daily_time_cost
GROUP BY link_dir, tod;