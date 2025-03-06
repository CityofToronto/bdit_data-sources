CREATE OR REPLACE VIEW traffic.svc_daily_totals AS

SELECT
    suv.study_id,
    suv.count_date,
    suv.direction,
    meta.centreline_id,
    cl.geom AS centreline_geom,
    SUM(suv.volume) AS daily_volume
FROM traffic.svc_unified_volumes AS suv
JOIN traffic.svc_metadata AS meta USING (study_id)
JOIN gis_core.centreline_latest AS cl USING (centreline_id)
GROUP BY
    suv.study_id,
    suv.count_date,
    suv.direction,
    meta.centreline_id,
    cl.geom
HAVING COUNT(*) = 4 * 24; --15 minute bins

ALTER VIEW traffic.svc_daily_totals OWNER TO traffic_admins;

GRANT SELECT ON VIEW traffic.svc_daily_totals TO bdit_humans;

COMMENT ON VIEW traffic.svc_daily_totals IS
'A daily summary of traffic.svc_unified_volumes by leg and centreline_id. 
Only rows with data for every 15 minute timebin are included. ';
