CREATE OR REPLACE FUNCTION mohan.detector_status(
	insert_value date
)
	returns table(
		reader_name text,
		--an_id bigint,
		data_last_aggregated date,
		reader_status text
		--ddown int,

)	
language plpgsql
	as $$
	
	begin
		return query
			with x AS 
			(select DISTINCT(analysis_id), MAX (datetime_bin::date) as last_reported,
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
	,b AS(
SELECT
		analysis_id, start_detector AS detector_name, route_status, last_reported
from 
z)
, c AS (
	SELECT * 
from b
UNION
SELECT
analysis_id, end_detector as detector_name,route_status, last_reported 
from 
z
)
, active AS (
SELECT DISTINCT (detector_name), MAX(last_reported), route_status 
FROM c
WHERE route_status = 'active'
GROUP BY route_status, detector_name --analysis_id,
)	
	SELECT DISTINCT (detector_name),  MAX(last_reported), route_status
FROM c
WHERE route_status = 'inactive' and detector_name NOT IN (SELECT detector_name from active)	
GROUP BY route_status, detector_name
UNION
SELECT *
from active
ORDER BY detector_name
	;
end; $$