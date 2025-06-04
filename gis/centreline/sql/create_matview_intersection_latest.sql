CREATE MATERIALIZED VIEW gis_core.intersection_latest AS

SELECT *
FROM gis_core.intersection
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
        FROM gis_core.intersection
    );

CREATE INDEX gis_core_intersection_latest_geom ON gis_core.intersection_latest
USING gist (geom);

CREATE UNIQUE INDEX centreline_latest_objectid_unique
ON gis_core.intersection_latest USING btree(
    objectid ASC
);

ALTER MATERIALIZED VIEW gis_core.intersection_latest OWNER TO gis_admins;

GRANT SELECT ON gis_core.intersection_latest TO bdit_humans, bdit_bots;

COMMENT ON MATERIALIZED VIEW gis_core.intersection_latest IS E''
'Materialized view containing the latest version of intersection,'
'derived from gis_core.intersection.'