--temporarily drop trigger
DROP TRIGGER IF EXISTS add_bluetooth_path_geom_trigger ON bluetooth.itsc_tt_paths;

--use this script to initially populated itsc_tt_paths.geom column, or for mass updates.
WITH centrelines AS (
    SELECT DISTINCT ON (division_id, path_id, start_timestamp, ordinality)
        paths.division_id,
        paths.path_id,
        paths.start_timestamp,
        ordinality,
        centreline.geom
    FROM bluetooth.itsc_tt_paths AS paths,
    UNNEST(centreline_ids) WITH ORDINALITY AS unnested (centreline_id, ordinality) 
    JOIN gis_core.centreline USING (centreline_id)
    ORDER BY
        paths.division_id,
        paths.path_id,
        paths.start_timestamp,
        unnested.ordinality,
        centreline.version_date DESC
),

updated AS (
    SELECT
        division_id,
        path_id,
        start_timestamp,
        ST_LineMerge(ST_Collect(geom ORDER BY ordinality)) AS new_geom
    FROM centrelines
    GROUP BY
        division_id,
        path_id,
        start_timestamp
)

UPDATE bluetooth.itsc_tt_paths
SET geom = new_geom
FROM updated
WHERE
    --these three fields make up the primary key
    updated.division_id = itsc_tt_paths.division_id
    AND updated.path_id = itsc_tt_paths.path_id
    AND updated.start_timestamp = itsc_tt_paths.start_timestamp;

--readd geom update trigger
CREATE OR REPLACE TRIGGER add_bluetooth_path_geom_trigger
    BEFORE INSERT OR UPDATE 
    ON bluetooth.itsc_tt_paths
    FOR EACH ROW
    EXECUTE FUNCTION bluetooth.itsc_add_bluetooth_path_geom();
