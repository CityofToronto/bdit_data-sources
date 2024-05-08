CREATE TABLE IF NOT EXISTS miovision_api.centreline_miovision
(
    centreline_id bigint,
    intersection_uid integer,
    leg text COLLATE pg_catalog."default"
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.centreline_miovision OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.centreline_miovision FROM bdit_humans;

GRANT SELECT ON TABLE miovision_api.centreline_miovision TO bdit_humans, miovision_api_bot;

COMMENT ON TABLE miovision_api.centreline_miovision
IS E''
'Use for joining Miovision legs to the centreline. Join using: '
'`miovision_api.centreline_miovision LEFT JOIN gis_core.centreline_latest USING (centreline_id)`. '
'See volumes/miovision/sql/updates/update-centreline_miovision.sql for updates and checks.';