-- View: bluetooth.itsc_active_routes

-- DROP MATERIALIZED VIEW IF EXISTS bluetooth.itsc_active_routes;

CREATE MATERIALIZED VIEW IF NOT EXISTS bluetooth.itsc_active_routes
TABLESPACE pg_default
AS
WITH active_paths AS (
    SELECT DISTINCT itsc_tt_raw_pathdata.path_id,
    itsc_tt_raw_pathdata.division_id
    FROM bluetooth.itsc_tt_raw_pathdata
    WHERE itsc_tt_raw_pathdata.dt >= (CURRENT_DATE - 7)
)
SELECT DISTINCT ON (itsc_tt_paths.path_id, itsc_tt_paths.division_id) itsc_tt_paths.path_id,
    itsc_tt_paths.division_id,
    itsc_tt_paths.source_id,
    itsc_tt_paths.length_m,
    itsc_tt_paths.path_type,
    itsc_tt_paths.centreline_ids,
    st_linemerge(itsc_tt_paths.geom) AS geom
FROM bluetooth.itsc_tt_paths
JOIN active_paths USING (path_id, division_id)
WHERE itsc_tt_paths.end_timestamp IS NULL
ORDER BY
    itsc_tt_paths.path_id,
    itsc_tt_paths.division_id,
    itsc_tt_paths.start_timestamp DESC
WITH DATA;

ALTER TABLE IF EXISTS bluetooth.itsc_active_routes
OWNER TO bt_admins;

GRANT SELECT ON TABLE bluetooth.itsc_active_routes TO bdit_humans;
