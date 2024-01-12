WITH x AS (
SELECT DISTINCT analysis_id, pull_data, MAX(datetime_bin) AS last_reported
FROM 
bluetooth.all_analyses
--bluetooth.aggr_5min 
LEFT JOIN bluetooth.aggr_5min  USING (analysis_id)
GROUP BY all_analyses.analysis_id
	)
, y AS (	
SELECT analysis_id, route_points->(0)->>'name' AS from_detector, route_points->(1)->>'name' AS to_detector, last_reported,
CASE
WHEN last_reported > ('now'::text::date - '1 day'::interval)
THEN 'active'
ELSE 'inactive'
END
AS route_status
FROM bluetooth.all_analyses
LEFT JOIN x USING (analysis_id)
)
, z AS (
SELECT from_detector AS detector_name, last_reported, route_status
FROM y
UNION
SELECT to_detector AS detector_name, last_reported, route_status
FROM y
)
, active AS (
SELECT DISTINCT (detector_name), MAX(last_reported) AS last_signal_received_on, route_status
FROM z
WHERE route_status = 'active'
GROUP BY detector_name, route_status
)

SELECT DISTINCT (detector_name), MAX(last_reported) AS last_signal_received_on, route_status
FROM z
WHERE route_status = 'inactive' AND detector_name NOT IN (SELECT detector_name FROM active)
GROUP BY detector_name, route_status
UNION
SELECT *
FROM active
ORDER BY detector_name;