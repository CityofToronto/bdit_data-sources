CREATE OR REPLACE VIEW miovision_api.restricted_entrance_legs AS
--which legs are "restricted"
SELECT
    intersection_uid,
    intersection_name,
    UNNEST(array_remove(ARRAY[
        CASE WHEN e_leg_restricted IS True THEN 'E' END,
        CASE WHEN n_leg_restricted IS True THEN 'N' END,
        CASE WHEN s_leg_restricted IS True THEN 'S' END,
        CASE WHEN w_leg_restricted IS True THEN 'W' END
    ], NULL)) AS leg
FROM miovision_api.active_intersections
ORDER BY intersection_uid;

ALTER VIEW miovision_api.restricted_entrance_legs OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.restricted_entrance_legs TO bdit_humans;

COMMENT ON VIEW miovision_api.restricted_entrance_legs
IS 'A list of Miovision intersections legs with no entrances allowed (either leg does not exist or one way outbound).';
