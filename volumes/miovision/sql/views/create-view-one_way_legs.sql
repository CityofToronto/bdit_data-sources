CREATE OR REPLACE VIEW miovision_api.one_way_legs AS
SELECT
    i.intersection_uid,
    i.intersection_name,
    i.int_id,
    clm.leg,
    CASE cl.oneway_dir_code
        WHEN -1 THEN
            CASE i.int_id
                WHEN cl.from_intersection_id THEN 'One way - entrance'
                WHEN cl.to_intersection_id THEN 'One way - exit'
            END
        WHEN 1 THEN
            CASE i.int_id
                WHEN cl.to_intersection_id THEN 'One way - entrance'
                WHEN cl.from_intersection_id THEN 'One way - exit'
            END
    END AS leg_direction,
    cl.from_intersection_id,
    cl.to_intersection_id,
    cl.oneway_dir_code,
    cl.oneway_dir_code_desc
FROM miovision_api.intersections AS i
LEFT JOIN miovision_api.centreline_miovision AS clm USING (intersection_uid)
LEFT JOIN gis_core.centreline_latest AS cl USING (centreline_id)
WHERE
    cl.oneway_dir_code <> 0
    AND i.date_decommissioned IS NULL
    AND i.intersection_uid NOT IN (125, 170)
ORDER BY i.intersection_uid;

ALTER VIEW miovision_api.one_way_legs OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.one_way_legs TO bdit_humans;

COMMENT ON VIEW miovision_api.one_way_legs
IS 'A list of Miovision intersections legs which are one way,
and their direction (entrance/exit).';
