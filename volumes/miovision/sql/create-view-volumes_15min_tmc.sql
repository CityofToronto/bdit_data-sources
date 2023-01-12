CREATE VIEW miovision_api.volumes_15min_tmc AS (
    SELECT *
    FROM miovision_api.volumes_15min_mvt
    WHERE NOT (classification_uid IN (6, 7, 10))
);

ALTER TABLE miovision_api.volumes_15min_tmc
    OWNER TO miovision_admins;

COMMENT ON VIEW miovision_api.volumes_15min_tmc
    IS 'miovision_api.volumes_15min_mvt, but only including turning movement counts.';

GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO bdit_humans;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO bdit_bots;
GRANT TRIGGER, REFERENCES, SELECT ON TABLE miovision_api.volumes_15min_tmc TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO dbadmin;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO miovision_admins;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO rds_superuser WITH GRANT OPTION;
