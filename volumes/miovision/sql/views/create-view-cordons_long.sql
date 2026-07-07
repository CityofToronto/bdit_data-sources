CREATE OR REPLACE VIEW miovision_api.cordons_long AS

SELECT
    cordons.camera_group,
    cordons.label,
    intersection_uid,
    exit_leg
FROM miovision_api.cordons,
UNNEST(intersection_uids) AS intersection_uid,
UNNEST(exit_legs) AS exit_leg;

ALTER VIEW miovision_api.cordons_long OWNER TO miovision_admins;

GRANT SELECT ON TABLE miovision_api.cordons_long TO bdit_Humans;

COMMENT ON VIEW miovision_api.cordons_long
IS 'Long format of miovision_api.cordons.';
