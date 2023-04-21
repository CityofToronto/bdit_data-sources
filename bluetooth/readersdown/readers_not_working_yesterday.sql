CREATE OR REPLACE VIEW bluetooth.readers_not_working_yesterday
AS
WITH x AS (
    SELECT
        aggr_5min.id,
        aggr_5min.analysis_id,
        aggr_5min.datetime_bin,
        aggr_5min.tt,
        aggr_5min.obs
    FROM bluetooth.aggr_5min
    WHERE aggr_5min.datetime_bin > ('now'::text::date - '1 day'::interval)
),

z AS (
    SELECT
        a.analysis_id,
        b.device_class_set_name,
        b.route_name,
        b.pull_data
    FROM bluetooth.aggr_5min AS a
    LEFT JOIN bluetooth.all_analyses AS b USING (analysis_id)
    WHERE a.datetime_bin > ('now'::text::date - '1 day'::interval) AND b.pull_data = TRUE
    GROUP BY a.analysis_id, b.device_class_set_name, b.route_name, b.pull_data
    HAVING count(a.datetime_bin) > 0
)

SELECT c.detector_name
FROM x
RIGHT JOIN jchew.bt_name_analysis_id AS c USING (analysis_id)
WHERE x . * IS NULL
EXCEPT
SELECT d.detector_name
FROM z
LEFT JOIN jchew.bt_name_analysis_id AS d USING (analysis_id, device_class_set_name, route_name)
ORDER BY 1;
