--identifying routes that did not send signals as of '......days ago' before current date  
WITH x AS (
	SELECT all_analyses.analysis_id AS xaid
	FROM bluetooth.all_analyses
	WHERE all_analyses.pull_data
	EXCEPT
		SELECT DISTINCT analysis_id
		FROM bluetooth.aggr_5min
		WHERE datetime_bin > ('now'::text::date - '2 day'::interval)
	ORDER BY xaid
),
-- analysis id and last reported date for routes that are/were active as of now 
 
y AS (
 SELECT DISTINCT aggr_5min.analysis_id AS yaid,
    MAX(aggr_5min.datetime_bin) AS last_reported
   FROM bluetooth.aggr_5min
  GROUP BY aggr_5min.analysis_id
	ORDER BY yaid

	),
--extract from_detector, to_detector and classify then as active or inactive
z AS (

SELECT analysis_id, route_points->(0)->>'name' AS from_detector, route_points->(1)->>'name' AS to_detector,
CASE
WHEN analysis_id IN (SELECT * FROM x) THEN 'inactive'
ELSE 'active'
END
AS route_status
FROM bluetooth.all_analyses
),

--joining to attach the last_date of data received to detector table
b AS 
(
SELECT z.analysis_id, z.from_detector, z.to_detector, z.route_status, y.last_reported
FROM z
LEFT JOIN y ON y.yaid = z.analysis_id
)

--selecting the list of inactive detectors based on inactive "from_detectors" 
SELECT b.from_detector AS detector_name, MAX(b.last_reported) AS last_signal_received, b.route_status AS detector_status
FROM b
WHERE route_status = 'inactive' AND from_detector NOT IN (
	SELECT from_detector
	FROM b 
	WHERE route_status = 'active')
GROUP BY b.from_detector, b.route_status


--selecting the list of inactive detectors based on inactive "to_detectors" 
SELECT b.to_detector AS detector_name, MAX(b.last_reported) AS last_signal_received, b.route_status AS detector_status
FROM b
WHERE route_status = 'inactive' AND to_detector NOT IN (
	SELECT to_detector
	FROM b 
	WHERE route_status = 'active')
GROUP BY b.to_detector, b.route_status


--now the challenge is to identify the difference between list of inactive detectors derived from "from_detectors and derived from "to_detectors" and combine them into one table....