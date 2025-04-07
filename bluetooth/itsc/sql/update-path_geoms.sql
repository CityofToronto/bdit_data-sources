WITH updated AS (
    SELECT
        paths.division_id,
        paths.path_id,
        paths.start_timestamp,
        st_union(lat.geom) AS new_geom
    FROM bluetooth.itsc_tt_paths AS paths,
        LATERAL (
            SELECT DISTINCT ON (centreline.centreline_id)
                centreline.centreline_id,
                centreline.geom
            FROM gis_core.centreline
            WHERE centreline.centreline_id = ANY(centreline.centreline_ids)
            ORDER BY centreline.centreline_id ASC, centreline.version_date DESC
        ) AS lat
    GROUP BY
        paths.division_id,
        paths.path_id,
        paths.start_timestamp
)

UPDATE bluetooth.itsc_tt_paths
SET geom = new_geom
FROM updated
WHERE
    --these three fields make up the primary key
    updated.division_id = itsc_tt_paths.division_id
    AND updated.path_id = itsc_tt_paths.path_id
    AND updated.start_timestamp = itsc_tt_paths.start_timestamp;