CREATE TABLE here.traffic_pattern_yy_v_b_yyyyqq AS 

WITH hourly_time_cost AS (
    SELECT
        routing_streets.link_dir,
        dt,
        datetime_bin(tod, 60) AS hr,
        harmean(ta.pct_50) AS daily_cost
    FROM here.routing_streets_yy_q AS routing_streets
    LEFT JOIN here.ta USING (link_dir)
    LEFT JOIN ref.holiday USING (dt)
    WHERE
        dt >= _start_date AND dt < _end_date AND
        EXTRACT(isodow FROM dt) IN (2,3,4) AND -- only include tues-thurs traffic
        holiday.dt IS NULL -- excluding holidays
    GROUP BY routing_streets.link_dir, dt, hr
)

SELECT 
    link_dir, 
    hr,
    harmean(daily_cost)::INT AS cost
FROM daily_time_cost
GROUP BY link_dir, hr;