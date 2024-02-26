CREATE MATERIALIZED VIEW gis_core.centreline_intersection_point_latest AS

SELECT *
FROM gis_core.centreline_intersection_point
WHERE version_date = (SELECT MAX(version_date) FROM gis_core.centreline_intersection_point);

CREATE TRIGGER refresh_trigger
AFTER INSERT OR UPDATE OR DELETE
ON gis_core.centreline_intersection_point
FOR EACH STATEMENT
EXECUTE PROCEDURE gis_core.centreline_intersection_point_latest_trigger();

CREATE INDEX gis_core_centreline_intersection_point_latest_geom ON gis_core.centreline_intersection_point_latest USING gist (geom);

ALTER MATERIALIZED VIEW gis_core.centreline_intersection_point_latest OWNER TO gis_admins;

GRANT SELECT ON gis_core.centreline_intersection_point_latest TO bdit_humans;

COMMENT ON MATERIALIZED VIEW gis_core.centreline_intersection_point_latest IS 'Materialized view containing the latest version of centreline intersection point, derived from gis_core.centreline_intersection_point.'
