-- View: bluetooth.itsc_active_routes

-- DROP MATERIALIZED VIEW IF EXISTS bluetooth.itsc_active_routes;

CREATE MATERIALIZED VIEW IF NOT EXISTS bluetooth.itsc_active_routes
TABLESPACE pg_default
AS
WITH active_paths AS (
    SELECT
        itsc_tt_raw_pathdata.path_id,
        itsc_tt_raw_pathdata.division_id,
        AVG(travel_time_s) AS avg_tt,
        AVG(travel_time_s) FILTER (WHERE date_part('hour', dt) IN (16, 17)) AS avg_tt_5_6pm,
        --CASE WHEN SUM(num_samples) > 0 THEN SUM(num_samples * travel_time_s)/SUM(num_samples) END AS weighted_avg_tt,
        SUM(num_samples) AS num_samples
    FROM bluetooth.itsc_tt_raw_pathdata
    WHERE itsc_tt_raw_pathdata.dt >= (CURRENT_DATE - 7)
    GROUP BY
        itsc_tt_raw_pathdata.path_id,
        itsc_tt_raw_pathdata.division_id
)

SELECT DISTINCT ON (itsc_tt_paths.path_id, itsc_tt_paths.division_id)
    itsc_tt_paths.path_id,
    itsc_tt_paths.division_id,
    itsc_tt_paths.source_id,
    itsc_tt_paths.length_m,
    itsc_tt_paths.centreline_ids,
    ST_LineFromEncodedPolyline(itsc_tt_paths.encoded_polyline) AS geom,
    itsc_tt_paths.length_m / active_paths.avg_tt * 3.6 AS avg_kph,
    itsc_tt_paths.length_m / active_paths.avg_tt_5_6pm * 3.6 AS avg_kph_5_6pm,
    --itsc_tt_paths.length_m / active_paths.weighted_avg_tt * 3.6 AS weighted_avg_kph,
    active_paths.avg_tt,
    --active_paths.weighted_avg_tt,
    active_paths.num_samples,
    itsc_tt_paths.start_node,
    itsc_tt_paths.end_node
FROM bluetooth.itsc_tt_paths
JOIN active_paths USING (path_id, division_id)
WHERE itsc_tt_paths.end_timestamp IS NULL

UNION

SELECT
    analysis_id AS path_id,
    null AS division_id,
    name AS source_id,
    length AS length_m,
    null AS centreline_ids,
    geom,
    length / AVG(tt) * 3.6 AS avg_kph,
    length / AVG(tt) FILTER (WHERE date_part('hour', datetime_bin) IN (16, 17)) * 3.6 AS avg_kph_5_6pm,
     AVG(tt),
    SUM(obs) AS num_samples,
    st_startpoint(geom) AS start_node,
    st_endpoint(geom) AS end_node
FROM bluetooth.aggr_5min
JOIN bluetooth.routes_temp USING (analysis_id)
WHERE datetime_bin >= CURRENT_DATE - 7
GROUP BY
    analysis_id,
    name,
    length,
    geom
WITH DATA;

ALTER TABLE IF EXISTS bluetooth.itsc_active_routes
OWNER TO bt_admins;

GRANT SELECT ON TABLE bluetooth.itsc_active_routes TO bdit_humans;

--SELECT * FROM bluetooth.itsc_active_routes
