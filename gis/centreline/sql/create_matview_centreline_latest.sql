CREATE MATERIALIZED VIEW gis_core.centreline_latest AS

SELECT *
FROM gis_core.centreline
WHERE
    version_date = (
        SELECT MAX(version_date)
        FROM gis_core.centreline
    );

CREATE TRIGGER refresh_trigger
AFTER INSERT OR UPDATE OR DELETE
ON gis_core.centreline
FOR EACH STATEMENT
EXECUTE PROCEDURE gis_core.centreline_latest_trigger();

CREATE INDEX gis_core_centreline_latest_geom ON gis_core.centreline_latest USING gist (geom);

CREATE UNIQUE INDEX centreline_latest_unique
ON gis_core.centreline_latest USING btree(
    centreline_id ASC
);

ALTER MATERIALIZED VIEW gis_core.centreline_latest OWNER TO gis_admins;

GRANT SELECT ON gis_core.centreline_latest TO bdit_humans, bdit_bots;

COMMENT ON MATERIALIZED VIEW gis_core.centreline_latest IS E''
'Materialized view containing the latest version of centreline, derived from gis_core.centreline.';