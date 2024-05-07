CREATE TABLE IF NOT EXISTS miovision_api.miovision_centreline
(
    centreline_id bigint,
    intersection_uid integer,
    leg text COLLATE pg_catalog."default"
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.miovision_centreline OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.miovision_centreline FROM bdit_humans;

GRANT SELECT ON TABLE miovision_api.miovision_centreline TO bdit_humans, miovision_api_bot;

COMMENT ON TABLE miovision_api.miovision_centreline
IS E''
'Use for joining Miovision legs to the centreline. Join using: '
'`miovision_api.miovision_centreline LEFT JOIN gis_core.centreline_latest USING (centreline_id)`. '
'See volumes/miovision/sql/updates/update-miovision_centreline.sql for updates and checks.';