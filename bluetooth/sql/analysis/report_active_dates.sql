CREATE VIEW bluetooth.report_active_dates AS
SELECT analysis_id, report_name, MIN(datetime_bin)::DATE AS start_date, CASE WHEN MAX(datetime_bin)::DATE > '2018-04-01' THEN NULL ELSE MAX(datetime_bin)::DATE END as end_date
FROM bluetooth.aggr_5min
INNER JOIN bluetooth.all_analyses USING (analysis_id)
GROUP BY analysis_id, report_name
ORDER BY report_name, start_date