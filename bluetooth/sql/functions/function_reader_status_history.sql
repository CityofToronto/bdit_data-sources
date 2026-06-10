-- FUNCTION: bluetooth.reader_status_history(date)

-- DROP FUNCTION IF EXISTS bluetooth.reader_status_history(date);

CREATE OR REPLACE FUNCTION bluetooth.reader_status_history(
	_dt date)
    RETURNS TABLE(an_id bigint, last_received date, status text, ddown integer, start_route_point_id text, start_detector text, end_route_point_id text, end_detector text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
	
	BEGIN
		-- Return last reported date and route status for the executed date
		WITH status_info AS (
			SELECT      DISTINCT(analysis_id), 
                     CASE
                        WHEN MAX (datetime_bin::date)>= (_dt) then _dt
                        ELSE MAX (datetime_bin::date)
                     END as last_reported,
                     CASE
                        WHEN  MAX (datetime_bin::date)>= (_dt) then True
                        ELSE False
                     END AS route_status,
                     _dt::date AS dt

		   FROM        bluetooth.all_analyses
		   LEFT JOIN   bluetooth.aggr_5min USING (analysis_id) 
		   GROUP BY    analysis_id)
      
      -- Extract start and end route point id and detector name for each analysis_id
            -- Extract start and end route point id and detector name for each analysis_id
      , route_info AS (
         SELECT      status_info.analysis_id,
                     status_info.last_reported,
                     status_info.route_status,
                     status_info.dt,
                     (all_analyses.route_points -> 0) ->> 'id'::text AS start_route_point_id,
                     (all_analyses.route_points -> 0) ->> 'name'::text AS start_detector,
                     (all_analyses.route_points -> 1) ->> 'id'::text AS end_route_point_id,
                     (all_analyses.route_points -> 1) ->> 'name'::text AS end_detector

         FROM        status_info
         INNER JOIN  bluetooth.all_analyses USING (analysis_id))

      -- Create one row for the start and end detector for each analysis_id
		, detector_list AS (
         SELECT      route_info.analysis_id,
                     route_info.start_detector AS detector_name,
                     route_info.route_status,
                     route_info.last_reported,
                     route_info.dt
         FROM        route_info
         UNION
         SELECT      route_info.analysis_id,
                     route_info.end_detector AS detector_name,
                     route_info.route_status,
                     route_info.last_reported,
			         route_info.dt
         FROM        route_info)

      -- Select only the detectors that are active
		, active AS (
         SELECT      DISTINCT reader_id,
                     max(detector_list.last_reported) AS last_reported,
                     detector_list.route_status,
                     detector_list.dt

         FROM        detector_list
         INNER JOIN  bluetooth.readers_history_corrected reader_history ON detector_list.detector_name = reader_history.read_name 
         WHERE       detector_list.route_status = TRUE
         GROUP BY    detector_list.route_status, reader_history.reader_id, detector_list.dt)
	
	-- Update reader_locations' date_last_received with active reader's last reported date 
		 , update_cte AS (
		 UPDATE bluetooth.readers_history_corrected reader_locations
   		set date_last_received = (SELECT DISTINCT max(last_reported) from active where active.reader_id = reader_locations.reader_id)
		 )
      	
		-- Insert into reader_status_history table
   	INSERT INTO  bluetooth.reader_status_history (reader_id, last_active_date, active, dt)
		
        SELECT       DISTINCT reader_id,
                     max(detector_list.last_reported) AS last_reported,
                     detector_list.route_status,
			         detector_list.dt
					 
         FROM        detector_list
         INNER JOIN  bluetooth.readers_history_corrected reader_history ON detector_list.detector_name = reader_history.read_name 
         WHERE       detector_list.route_status = FALSE AND 
                     NOT (reader_id IN (SELECT reader_id FROM active))
         GROUP BY    detector_list.route_status, reader_history.reader_id, detector_list.dt

         UNION ALL
         
         SELECT      active.reader_id,
                     active.last_reported,
                     active.route_status,
                     active.dt
         FROM        active;

END;
$BODY$;

ALTER FUNCTION bluetooth.reader_status_history(date)
    OWNER TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.reader_status_history(date) TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.reader_status_history(date) TO bt_bot;

REVOKE ALL ON FUNCTION bluetooth.reader_status_history(date) FROM PUBLIC;
