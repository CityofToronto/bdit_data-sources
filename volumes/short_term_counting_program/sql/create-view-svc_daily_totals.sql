CREATE OR REPLACE VIEW traffic.svc_daily_totals AS

SELECT
    suv.study_id,
    suv.count_date,
    suv.direction,
    meta.midblock_id,
    cl.geom AS midblock_geom,
    SUM(suv.volume) AS daily_volume,
    centreline_id_array
FROM traffic.svc_unified_volumes AS suv
JOIN traffic.svc_metadata AS meta USING (study_id)
JOIN traffic.centreline2_midblocks AS cl USING (midblock_id)
GROUP BY
    suv.study_id,
    suv.count_date,
    suv.direction,
    meta.midblock_id,
    centreline_id_array,
    cl.geom
HAVING COUNT(*) = 4 * 24; --15 minute bins

ALTER VIEW traffic.svc_daily_totals OWNER TO traffic_admins;

GRANT SELECT ON TABLE traffic.svc_daily_totals TO bdit_humans;

COMMENT ON VIEW traffic.svc_daily_totals
IS 'A daily summary of `traffic.svc_unified_volumes` by `direction` and `midblock_id`. Only rows with data for every 15 minute timebin are included.';
