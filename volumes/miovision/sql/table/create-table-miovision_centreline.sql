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
'`miovision_api.miovision_centreline LEFT JOIN gis_core.centreline_latest USING (centreline_id)`'

--examine intersections with dupes
--91: Lakeshore and Spadina. These two western legs are actually legit. We could only differentiate by movement.
SELECT
    mio.centreline_id,
    mio.intersection_uid,
    mio.leg,
    cl.geom
FROM (
    SELECT DISTINCT
        intersection_uid,
        leg
    FROM miovision_api.miovision_centreline
    GROUP BY
        intersection_uid,
        leg
    HAVING COUNT(1) > 1
) dupes
LEFT JOIN miovision_api.miovision_centreline AS mio USING (intersection_uid)
LEFT JOIN gis_core.centreline_latest AS cl USING (centreline_id)
ORDER BY
    mio.intersection_uid,
    mio.leg;

--identified some intersections with missing legs that are in intersection_movements:
--78: Bloor and Kingsway is a 4 legged intersection, but the south leg is not in the centreline (private road)
--68: Steels and Jane, N leg is outside of TO.
WITH missing AS (
    SELECT DISTINCT
        intersection_uid,
        leg
    FROM miovision_api.intersection_movements
    WHERE classification_uid = 1
    EXCEPT
    SELECT DISTINCT
        intersection_uid,
        leg
    FROM miovision_api.miovision_centreline
)
SELECT
    missing.intersection_uid,
    missing.leg,
    intersections.geom
FROM missing
JOIN miovision_api.intersections USING (intersection_uid);
