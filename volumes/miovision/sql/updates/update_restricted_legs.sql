--identify restricted legs that are actually just one way
SELECT
    intersection_uid,
    intersection_name,
    UNNEST(array_remove(ARRAY[
        CASE WHEN e_leg_restricted IS TRUE THEN 'E' END,
        CASE WHEN n_leg_restricted IS TRUE THEN 'N' END,
        CASE WHEN s_leg_restricted IS TRUE THEN 'S' END,
        CASE WHEN w_leg_restricted IS TRUE THEN 'W' END
    ], NULL
    ) AS leg,
    geom
FROM miovision_api.active_intersections

INTERSECT

SELECT intersection_uid, ai.intersection_name, leg, geom
FROM miovision_api.one_way_legs
JOIN miovision_api.active_intersections AS ai USING (intersection_uid)
ORDER BY intersection_uid;

--update to NULL
UPDATE miovision_api.intersections SET e_leg_restricted = NULL
WHERE intersection_uid = 1 AND intersection_name = 'Adelaide / Bathurst';
UPDATE miovision_api.intersections SET e_leg_restricted = NULL
WHERE intersection_uid = 2 AND intersection_name = 'Adelaide / Spadina';
UPDATE miovision_api.intersections SET e_leg_restricted = NULL
WHERE intersection_uid = 3 AND intersection_name = 'Adelaide / Bay';
UPDATE miovision_api.intersections SET e_leg_restricted = NULL
WHERE intersection_uid = 4 AND intersection_name = 'Adelaide / Jarvis';
UPDATE miovision_api.intersections SET w_leg_restricted = NULL
WHERE intersection_uid = 27 AND intersection_name = 'Richmond / Spadina';
UPDATE miovision_api.intersections SET w_leg_restricted = NULL
WHERE intersection_uid = 28 AND intersection_name = 'Richmond / Bay';
UPDATE miovision_api.intersections SET w_leg_restricted = NULL
WHERE intersection_uid = 29 AND intersection_name = 'Richmond / Jarvis';
UPDATE miovision_api.intersections SET w_leg_restricted = NULL
WHERE intersection_uid = 31 AND intersection_name = 'Wellington / Bay';
UPDATE miovision_api.intersections SET n_leg_restricted = NULL
WHERE intersection_uid = 43 AND intersection_name = 'Danforth / Logan';
UPDATE miovision_api.intersections SET e_leg_restricted = NULL
WHERE intersection_uid = 91 AND intersection_name = 'Lake Shore / Spadina';
UPDATE miovision_api.intersections SET w_leg_restricted = NULL
WHERE intersection_uid = 95 AND intersection_name = 'Wellington / Bathurst';
UPDATE miovision_api.intersections SET n_leg_restricted = NULL
WHERE intersection_uid = 106 AND intersection_name = 'Eglinton / Allen North';
UPDATE miovision_api.intersections SET e_leg_restricted = NULL
WHERE intersection_uid = 112 AND intersection_name = 'Niagara / Bathurst';
UPDATE miovision_api.intersections SET w_leg_restricted = NULL
WHERE intersection_uid = 180 AND intersection_name = 'Denton / Victoria Park';
