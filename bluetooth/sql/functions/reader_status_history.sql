-- FUNCTION: mohan.reader_status_history(date)

-- DROP FUNCTION mohan.reader_status_history(date);

CREATE OR REPLACE FUNCTION mohan.reader_status_history(
	insert_value date)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$	
	begin
		with x AS 
			(select DISTINCT(analysis_id), 
			 CASE
			 WHEN MAX (datetime_bin::date)>= (insert_value-1) then (insert_value-1)
			 ELSE MAX (datetime_bin::date)
			 END 
			 as last_reported,
			CASE
			WHEN  MAX (datetime_bin::date)>= (insert_value-1) then 'True'::text
		else 'False'::text
		END 
		AS route_status
			 ,CASE
		WHEN max(aggr_5min.datetime_bin::date) >= (insert_value::text::date - 1) THEN insert_value::date
                    ELSE (insert_value::date)
		END AS dt
		from 
		bluetooth.all_analyses
		LEFT JOIN bluetooth.aggr_5min USING (analysis_id) 
		GROUP BY analysis_id)
		, y AS (
		 SELECT all_analyses.analysis_id,
    (all_analyses.route_points -> 0) ->> 'id'::text AS start_route_point_id,
    (all_analyses.route_points -> 0) ->> 'name'::text AS start_detector,
    (all_analyses.route_points -> 1) ->> 'id'::text AS end_route_point_id,
    (all_analyses.route_points -> 1) ->> 'name'::text AS end_detector
   FROM bluetooth.all_analyses
		)
  , z AS (
         SELECT x.analysis_id,
            x.last_reported,
            x.route_status,
            x.dt,
            y.start_route_point_id,
            y.start_detector,
            y.end_route_point_id,
            y.end_detector
           FROM x
             LEFT JOIN y USING (analysis_id)
        )
		, b AS (
         SELECT z.analysis_id,
            z.start_detector AS detector_name,
            z.route_status,
            z.last_reported,
			z.dt
           FROM z
        )
		, c AS (
         SELECT b.analysis_id,
            b.detector_name,
            b.route_status,
            b.last_reported,
			b.dt
           FROM b
        UNION
         SELECT z.analysis_id,
            z.end_detector AS detector_name,
            z.route_status,
            z.last_reported,
			z.dt
           FROM z
        )
		, active AS (
         SELECT DISTINCT c.detector_name,
            max(c.last_reported) AS max,
            c.route_status,
            detectors_history_final.reader_id as id,
			c.dt
           FROM c
             LEFT JOIN detectors_history_final ON c.detector_name = detectors_history_final.read_name::text
          WHERE c.route_status = 'True'::text
          GROUP BY c.route_status, c.detector_name, detectors_history_final.reader_id, c.dt
        ), final as (
 SELECT DISTINCT c.detector_name,
    max(c.last_reported) AS max,
    c.route_status,
    detectors_history_final.reader_id,
			c.dt
   FROM c
     LEFT JOIN detectors_history_final ON c.detector_name = detectors_history_final.read_name::text
  WHERE c.route_status = 'False'::text AND NOT (c.detector_name IN ( SELECT active.detector_name
           FROM active))
  GROUP BY c.route_status, c.detector_name, detectors_history_final.reader_id, c.dt
UNION
 SELECT active.detector_name,
    active.max,
    active.route_status,
    active.id,
	active.dt
   FROM active)
   
   INSERT INTO mohan.reader_status_history (reader_id, last_active_date, active, dt)
   SELECT DISTINCT reader_id, max(max),route_status::bool, dt
   from final
   where reader_id IS NOT NULL
   group by reader_id, route_status, dt
   ORDER BY reader_id

	;
end; $BODY$;

ALTER FUNCTION mohan.reader_status_history(date)
    OWNER TO mohan;
