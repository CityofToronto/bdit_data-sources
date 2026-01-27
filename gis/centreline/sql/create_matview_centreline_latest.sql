CREATE MATERIALIZED VIEW gis_core.centreline_latest AS

SELECT *
FROM gis_core.centreline
WHERE
    version_date = (
        SELECT MAX(centreline.version_date)
        FROM gis_core.centreline
    )
    AND feature_code_desc IN (
        'Expressway',
        'Expressway Ramp',
        'Major Arterial',
        'Major Arterial Ramp',
        'Minor Arterial',
        'Minor Arterial Ramp',
        'Collector',
        'Collector Ramp',
        'Local',
        'Access Road',
        'Other',
        'Other Ramp',
        'Laneway',
        'Pending'
    );

CREATE INDEX gis_core_centreline_latest_geom ON gis_core.centreline_latest USING gist (geom);

CREATE UNIQUE INDEX centreline_latest_unique
ON gis_core.centreline_latest USING btree (
    centreline_id ASC
);

ALTER MATERIALIZED VIEW gis_core.centreline_latest OWNER TO gis_admins;

GRANT SELECT ON gis_core.centreline_latest TO bdit_humans, bdit_bots;

COMMENT ON MATERIALIZED VIEW gis_core.centreline_latest IS E''
'Materialized view containing the latest version of centreline, derived from gis_core.centreline, excluding Busway and Trail.';