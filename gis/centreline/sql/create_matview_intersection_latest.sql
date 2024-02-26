CREATE MATERIALIZED VIEW gis_core.intersection_latest AS

SELECT *
FROM gis_core.intersection
WHERE version_date = (SELECT MAX(version_date) FROM gis_core.intersection);

CREATE TRIGGER refresh_trigger
AFTER INSERT OR UPDATE OR DELETE
ON gis_core.intersection
FOR EACH STATEMENT
EXECUTE PROCEDURE gis_core.intersection_latest_trigger();

CREATE INDEX gis_core_intersection_latest_geom ON gis_core.intersection_latest USING gist (geom);

ALTER MATERIALIZED VIEW gis_core.intersection_latest OWNER TO gis_admins;

GRANT SELECT ON gis_core.intersection_latest TO bdit_humans;

COMMENT ON MATERIALIZED VIEW gis_core.intersection_latest IS 'Materialized view containing the latest version of intersection , derived from gis_core.intersection.'