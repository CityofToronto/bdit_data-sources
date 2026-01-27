CREATE MATERIALIZED VIEW gis_core.centreline_intersection_point_latest AS

--in case of duplicate intersection_id (rare), only take the latest by objectid
SELECT DISTINCT ON (intersection_id) *
FROM gis_core.centreline_intersection_point
WHERE
    intersection_id IN (
        SELECT from_intersection_id
        FROM gis_core.centreline_latest
        UNION
        SELECT to_intersection_id
        FROM gis_core.centreline_latest
    )
    AND version_date = (
        SELECT MAX(version_date)
        FROM gis_core.centreline_intersection_point
    )
ORDER BY intersection_id ASC, objectid DESC;

CREATE INDEX gis_core_centreline_intersection_point_latest_geom
ON gis_core.centreline_intersection_point_latest USING gist (geom);

CREATE UNIQUE INDEX centreline_intersection_point_latest_unique
ON gis_core.centreline_intersection_point_latest USING btree (
    intersection_id ASC
);

ALTER MATERIALIZED VIEW gis_core.centreline_intersection_point_latest OWNER TO gis_admins;

GRANT SELECT ON gis_core.centreline_intersection_point_latest TO bdit_humans, bdit_bots;

COMMENT ON MATERIALIZED VIEW gis_core.centreline_intersection_point_latest IS E''
'Materialized view containing the latest version of centreline intersection point,'
'derived from gis_core.centreline_intersection_point. Removes some (rare) duplicate intersection_ids.'
