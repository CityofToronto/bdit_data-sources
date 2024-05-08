--use this script to prepare new inserts for the miovision_centreline table.

--use a temp table so you can examine the results before inserting.
DROP TABLE miovision_centreline_temp;

CREATE TEMP TABLE miovision_centreline_temp AS (
    --find all the nodes in the centreline file that touch miovision intersections.
    --You have to match with "from" and "to" nodes to get all the segments
    WITH cent_int AS (
        --to nodes
        SELECT DISTINCT
            i.intersection_uid,
            i.lat AS int_lat,
            i.lng AS int_lng,
            cl.centreline_id,
            ci.latitude AS cl_lat,
            ci.longitude AS cl_lng,
            cl.geom AS cl_geom
        FROM miovision_api.intersections AS i
        LEFT JOIN gis_core.centreline_latest AS cl
            ON i.int_id = cl.to_intersection_id
            AND cl.feature_code_desc NOT IN (
                'Trail', 'Geostatistical line', 'Other', 'River', 'Major Railway', 'Hydro Line',
                'Walkway', 'Major Shoreline', 'Creek/Tributary', 'Ferry Route', 'Minor Railway',
                'Minor Shoreline (Land locked)'
            )
        LEFT JOIN gis_core.centreline_intersection_point_latest AS ci
            ON cl.from_intersection_id = ci.intersection_id
        --adjust intersection filter (in below UNION as well)
        WHERE i.intersection_uid IN (...) --noqa: PRS

        UNION ALL

        --from nodes
        SELECT DISTINCT
            i.intersection_uid,
            i.lat AS int_lat,
            i.lng AS int_lng,
            cl.centreline_id,
            ci.latitude AS cl_lat,
            ci.longitude AS cl_lng,
            cl.geom AS cl_geom
        FROM miovision_api.intersections AS i
        LEFT JOIN gis_core.centreline_latest AS cl
            ON i.int_id = cl.from_intersection_id
            AND cl.feature_code_desc NOT IN (
                'Trail', 'Geostatistical line', 'Other', 'River', 'Major Railway', 'Hydro Line',
                'Walkway', 'Major Shoreline', 'Creek/Tributary', 'Ferry Route', 'Minor Railway',
                'Minor Shoreline (Land locked)'
            )
        LEFT JOIN gis_core.centreline_intersection_point_latest AS ci
            ON cl.to_intersection_id = ci.intersection_id
        --adjust intersection filter
        WHERE i.intersection_uid IN (...) --noqa: PRS
    ),

    --determine difference in the "non-miovision node" co-ordinates - figure out if
    --we're dealing with the north-south or east-west street in the intersection
    ll_diff AS (
        SELECT
            intersection_uid,
            int_lat,
            int_lng,
            centreline_id,
            cl_lat,
            cl_lng,
            cl_lat - int_lat AS lat_diff,
            cl_lng - int_lng AS lng_diff,
            cl_geom,
            CASE
                WHEN abs(cl_lat - int_lat) > abs(cl_lng - int_lng) THEN 'NS'
                WHEN abs(cl_lng - int_lng) > abs(cl_lat - int_lat) THEN 'EW'
            END AS nsew
        FROM cent_int
    )

    --determine which segments are N, S, E, W so that we
    --can match with miovision leg + intersection_uid
    SELECT
        centreline_id,
        intersection_uid,
        CASE
            WHEN nsew = 'NS' AND cl_lat > int_lat THEN 'N'
            WHEN nsew = 'NS' AND cl_lat < int_lat THEN 'S'
            WHEN nsew = 'EW' AND cl_lng > int_lng THEN 'E'
            WHEN nsew = 'EW' AND cl_lng < int_lng THEN 'W'
        END AS leg,
        cl_geom
    FROM ll_diff
    ORDER BY intersection_uid
);

--examine results:
SELECT
    centreline_id,
    intersection_uid,
    leg,
    cl_geom
FROM miovision_centreline_temp;

--check for intersections with duplicates and fix
SELECT mio.* --noqa
FROM (
    SELECT
        intersection_uid,
        leg
    FROM miovision_centreline_temp
    GROUP BY
        intersection_uid,
        leg
    HAVING COUNT(*) > 1
) AS dupes
JOIN miovision_centreline_temp AS mio USING (intersection_uid, leg)
ORDER BY
    mio.intersection_uid,
    mio.leg;

--make adjustments as needed: 
/*
DELETE FROM miovision_centreline_temp
WHERE ...
*/

/*
UPDATE miovision_centreline_temp
SET centreline =
WHERE ...
*/

--when ready, insert into miovision_centreline:
INSERT INTO miovision_api.miovision_centreline (centreline_id, intersection_uid, leg)
SELECT
    centreline_id,
    intersection_uid,
    leg
FROM miovision_centreline_temp;

--USE THE FOLLOWING SCRIPTS TO QC:

--examine intersections with dupes
--91: Lakeshore and Spadina. Both western legs are actually legit. Could differentiate by movement.
SELECT
    mio.centreline_id,
    mio.intersection_uid,
    mio.leg,
    cl.geom
FROM (
    SELECT
        intersection_uid,
        leg
    FROM miovision_api.miovision_centreline
    GROUP BY
        intersection_uid,
        leg
    HAVING COUNT(*) > 1
) AS dupes
LEFT JOIN miovision_api.miovision_centreline AS mio USING (intersection_uid)
LEFT JOIN gis_core.centreline_latest AS cl USING (centreline_id)
ORDER BY
    mio.intersection_uid,
    mio.leg;

--identified some intersections with missing legs that are in intersection_movements:
--78: Bloor and Kingsway is 4 legged, but the S leg is not in the centreline (private rd)
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