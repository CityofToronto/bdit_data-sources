WITH X AS (select analysis_id from bluetooth.segments

EXCEPT

select distinct(f.analysis_id) from (
SELECT user_id, analysis_id, measured_timestamp  FROM bluetooth.observations_201802
where measured_timestamp::date = '2018-02-28'
and analysis_id in (select analysis_id from bluetooth.segments) 
ORDER BY measured_timestamp DESC) f)

SELECT *
FROM bluetooth.segments
INNER JOIN X USING (analysis_id)