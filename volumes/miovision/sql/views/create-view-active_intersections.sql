-- View: miovision_api.active_intersections

-- DROP VIEW miovision_api.active_intersections;

CREATE OR REPLACE VIEW miovision_api.active_intersections AS
SELECT
    intersection_uid,
    id,
    intersection_name,
    date_installed,
    date_decommissioned,
    lat,
    lng,
    street_main,
    street_cross,
    int_id,
    px,
    geom,
    n_leg_restricted,
    e_leg_restricted,
    s_leg_restricted,
    w_leg_restricted,
    api_name
FROM miovision_api.intersections
WHERE
    date_decommissioned IS NULL
    AND date_installed IS NOT NULL;

ALTER TABLE miovision_api.active_intersections OWNER TO miovision_admins;
COMMENT ON VIEW miovision_api.active_intersections IS 'Just the active intersections.';

GRANT SELECT ON TABLE miovision_api.active_intersections TO bdit_humans;
GRANT ALL ON TABLE miovision_api.active_intersections TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.active_intersections TO miovision_api_bot;
