CREATE VIEW miovision_api.one_way_entrance_legs AS
SELECT
    intersection_uid,
    intersection_name,
    leg,
    int_id,
    from_intersection_id,
    to_intersection_id,
    oneway_dir_code,
    oneway_dir_code_desc
FROM
gis_core.centreline_latest
JOIN miovision_api.centreline_miovision USING (centreline_id)
JOIN miovision_api.intersections AS i USING (intersection_uid)
WHERE
    i.date_decommissioned IS NULL
    AND i.intersection_uid NOT IN (125, 170)
    AND ((
        oneway_dir_code = -1
        AND int_id = from_intersection_id
    ) OR (
        oneway_dir_code = 1
        AND int_id = to_intersection_id
    ))
ORDER BY intersection_uid;

ALTER VIEW miovision_api.one_way_entrance_legs OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.one_way_entrance_legs TO bdit_humans;

COMMENT ON VIEW miovision_api.one_way_entrance_legs
IS 'A list of Miovision intersections legs which are one way inbound (no vehicle exits allowed).';
