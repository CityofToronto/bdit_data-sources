CREATE MATERIALIZED VIEW gis_core.centreline_latest_all_feature AS

SELECT *
FROM gis_core.centreline
WHERE
    version_date = (
        SELECT MAX(centreline.version_date)
        FROM gis_core.centreline
    );


CREATE INDEX gis_core_centreline_latest_all_feature_geom ON gis_core.centreline_latest_all_feature USING gist (geom);

CREATE UNIQUE INDEX centreline_latest_all_feature_unique
ON gis_core.centreline_latest_all_feature USING btree(
    centreline_id ASC
);

ALTER MATERIALIZED VIEW gis_core.centreline_latest_all_feature OWNER TO gis_admins;

GRANT SELECT ON gis_core.centreline_latest_all_feature TO bdit_humans, bdit_bots;

COMMENT ON MATERIALIZED VIEW gis_core.centreline_latest_all_feature IS E''
'Materialized view containing the latest version of centreline with all feature code, derived from gis_core.centreline.';