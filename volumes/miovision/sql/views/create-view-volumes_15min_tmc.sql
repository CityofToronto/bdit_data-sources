CREATE VIEW miovision_api.volumes_15min_tmc AS (
    SELECT
        intersection_uid,
        datetime_bin,
        classification_uid,
        leg,
        movement_uid,
        volume
    FROM miovision_api.volumes_15min_mvt_filtered
    WHERE classification_uid NOT IN (6, 7, 10)
);

ALTER TABLE miovision_api.volumes_15min_tmc
OWNER TO miovision_admins;

COMMENT ON VIEW miovision_api.volumes_15min_tmc
IS 'miovision_api.volumes_15min_mvt_filtered, but only including turning movement counts.';

GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO bdit_humans;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO bdit_bots;
GRANT TRIGGER, REFERENCES, SELECT ON TABLE miovision_api.volumes_15min_tmc
TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO dbadmin;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO miovision_admins;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO rds_superuser WITH GRANT OPTION;