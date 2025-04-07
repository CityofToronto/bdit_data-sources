WITH updated AS (
    SELECT division_id, path_id, start_timestamp, st_union(lat.geom) AS new_geom
    FROM bluetooth.itsc_tt_paths,
    LATERAL (
        SELECT DISTINCT ON (centreline_id) centreline_id, centreline.geom
        FROM gis_core.centreline
        WHERE centreline.centreline_id = ANY(centreline_ids)
        ORDER BY centreline_id, version_date DESC
    ) AS lat
    GROUP BY 1, 2, 3
)

UPDATE bluetooth.itsc_tt_paths
SET geom = new_geom
FROM updated
WHERE
    --these three fields make up the primary key
    updated.division_id = itsc_tt_paths.division_id
    AND updated.path_id = itsc_tt_paths.path_id
    AND updated.start_timestamp = itsc_tt_paths.start_timestamp;