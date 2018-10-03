CREATE OR REPLACE FUNCTION bluetooth.update_dates_wo_data(dt DATE)
RETURNS void AS
$BODY$
BEGIN


    DELETE FROM bluetooth.dates_without_data
    WHERE day_without_data = dt;

    INSERT INTO bluetooth.dates_without_data
        SELECT all_analyses.analysis_id, date_with_data
    FROM bluetooth.all_analyses
    CROSS JOIN (SELECT dt AS date_with_data) d 
    LEFT OUTER JOIN bluetooth.observations obs ON all_analyses.analysis_id=obs.analysis_id AND measured_timestamp::DATE = date_with_data
    WHERE all_analyses.pull_data 
    GROUP BY all_analyses.analysis_id, date_with_data
    HAVING COUNT(obs.analysis_id) =0;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  ALTER FUNCTION bluetooth.update_dates_wo_data(DATE)
  OWNER TO bt_admins;
   GRANT EXECUTE ON FUNCTION bluetooth.update_dates_wo_data(DATE) TO aharpal;
   GRANT EXECUTE ON FUNCTION bluetooth.update_dates_wo_data(DATE) TO bt_insert_bot;
GRANT EXECUTE ON FUNCTION bluetooth.update_dates_wo_data(DATE) TO bt_admins;
COMMENT ON FUNCTION bluetooth.update_dates_wo_data(DATE) IS 'Update the dates_without_data table for dt ONLY';

CREATE OR REPLACE FUNCTION bluetooth.update_dates_wo_data(startdate DATE, enddate DATE)
RETURNS void AS
$BODY$
BEGIN
    IF enddate < startdate THEN
        RAISE EXCEPTION 'startdate cannot be after enddate';
	END IF;
    
    IF enddate = startdate THEN
        RAISE EXCEPTION 'startdate cannot be equal to enddate' 
            USING HINT = 'Use bluetooth.update_dates_wo_data(DATE) instead';
	END IF;
        
    DELETE FROM bluetooth.dates_without_data
    WHERE day_without_data >= startdate AND day_without_data <= enddate;

    INSERT INTO bluetooth.dates_without_data
        SELECT all_analyses.analysis_id, date_with_data
    FROM bluetooth.all_analyses
    CROSS JOIN (SELECT startdate + generate_series(0,enddate - startdate) * INTERVAL '1 Day' date_with_data) d 
    LEFT OUTER JOIN bluetooth.observations obs ON all_analyses.analysis_id=obs.analysis_id AND measured_timestamp::DATE = date_with_data
    WHERE all_analyses.pull_data 
    GROUP BY all_analyses.analysis_id, date_with_data
    HAVING COUNT(obs.analysis_id) =0;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  ALTER FUNCTION bluetooth.update_dates_wo_data(DATE, DATE)
  OWNER TO bt_admins;
   GRANT EXECUTE ON FUNCTION bluetooth.update_dates_wo_data(DATE, DATE) TO aharpal;
   GRANT EXECUTE ON FUNCTION bluetooth.update_dates_wo_data(DATE, DATE) TO bt_insert_bot;
GRANT EXECUTE ON FUNCTION bluetooth.update_dates_wo_data(DATE, DATE) TO bt_admins;
COMMENT ON FUNCTION bluetooth.update_dates_wo_data(DATE, DATE) IS 'Update the dates_without_data table from start date to end date INCLUSIVELY';