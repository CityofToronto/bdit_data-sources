--this is a bit of a convoluded process... here are the steps I took. I'm happy to make it more efficient
--1. wrote code to find the centreline_ids that correspond to the segments that touch miovision intersections; put these in a table
--2. did some error checking to see if that code worked (there were 9 situations where it didn't)...
--3. manually investigated and fixed the centrelines that needed to be fixed (in the table)

--1. Here is the code that finds centreline_ids that touch miovision intersections:
CREATE TABLE miovision_api.centerline_miovision AS (

    --find all the nodes in the centreline file that touch miovision intersections. You have to match with "from" and "to" nodes to get all the segments
    WITH cent_int AS (
        SELECT DISTINCT
            ter.intersection_uid,
            ter.lat,
            ter.lng,
            cent.centreline_id,
            ci.latitude,
            ci.longitude
        FROM miovision_api.intersections AS ter
        LEFT JOIN prj_volume.centreline AS cent ON ter.int_id = cent.to_intersection_id
        LEFT JOIN prj_volume.centreline_intersections AS ci ON cent.from_intersection_id = ci.intersection_id
        WHERE cent.feature_code_desc NOT IN ('Trail')

        UNION ALL

        SELECT DISTINCT
            ter.intersection_uid,
            ter.lat,
            ter.lng,
            cent.centreline_id,
            ci.latitude,
            ci.longitude
        FROM miovision_api.intersections AS ter
        LEFT JOIN prj_volume.centreline AS cent ON ter.int_id = cent.from_intersection_id
        LEFT JOIN prj_volume.centreline_intersections AS ci ON cent.to_intersection_id = ci.intersection_id
        WHERE cent.feature_code_desc NOT IN ('Trail')
    ),

    --determine difference in the "non-miovision node" co-ordinates - figure out if we're dealing with the north-south or east-west street in the intersection
    ll_diff AS (
        SELECT
            ci.intersection_uid,
            ci.lat,
            ci.lng,
            ci.centreline_id,
            ci.latitude,
            ci.longitude,
            ci.latitude - ci.lat AS lat_diff,
            ci.longitude - ci.lng AS lng_diff,
            CASE
                WHEN abs(ci.latitude - ci.lat) > abs(ci.longitude - ci.lng) THEN 'NS'
                WHEN abs(ci.longitude - ci.lng) > abs(ci.latitude - ci.lat) THEN 'EW'
            END AS nsew
        FROM cent_int AS ci
    ),

    --determine which segments are N, S, E, W so that we can match with miovision leg + intersection_uid
    cent_leg AS (
        SELECT
            ld.intersection_uid,
            ld.lat,
            ld.lng,
            ld.centreline_id,
            ld.latitude,
            ld.longitude,
            ld.nsew,
            CASE
                WHEN ld.nsew = 'NS' AND ld.latitude > ld.lat THEN 'N'
                WHEN ld.nsew = 'NS' AND ld.latitude < ld.lat THEN 'S'
                WHEN ld.nsew = 'EW' AND ld.longitude > ld.lng THEN 'E'
                WHEN ld.nsew = 'EW' AND ld.longitude < ld.lng THEN 'W'
            END AS leg
        FROM ll_diff AS ld
    )

    --yeah there's a ton of stuff we needed for calculations in those earlier tables but now we just need:
    SELECT
        cl.centreline_id,
        cl.intersection_uid,
        cl.leg
    FROM cent_leg AS cl
);

--2. It's important that the centreline to miovision file has the right combination of intersection_uids and legs. Here's how I checked that:
WITH data_legs AS (
    SELECT DISTINCT
        vol.intersection_uid,
        vol.leg
    FROM miovision_api.volumes_15min AS vol--this gets me all the distinct intersection_uid and leg combos in the actual dataset
)

SELECT
    cm.centreline_id,
    cm.intersection_uid,
    cm.leg,
    dl.intersection_uid AS data_int,
    dl.leg AS data_leg
FROM data_legs AS dl
FULL JOIN miovision_api.centerline_miovision AS cm ON cm.intersection_uid = dl.intersection_uid AND cm.leg = dl.leg -- full join means I can see what's in each pile
ORDER BY dl.intersection_uid;

-- When you run this, it will look as though two intersections, in addition to the 9 listed below, have data but no corresponding centreline_id. They are:
    -- 5W (Front + Bathurst - Front ends at Bathurst) has no volumes over 0 
    -- 37N (Danforth + Jones) - Jones ends at a strip mall on Danforth but there's nothing on the centreline to map to

--3. There were 9 cases where there was data for an intersection+leg combo that wasn't in my look up table, so I did some investigations and manually updated accordingly.
--of the 9 missing intersection+leg combos, two were for non-streets, so there's nothing to map them to in the centreline.
--the first two digits in forthcoming comments represent intersection_uid and leg

-- 1 W Adelaide jogs at Bathurst, the miovision node doesn't capture that so I added it
INSERT INTO miovision_api.centerline_miovision
VALUES (14073511, 1, 'W');

-- 24 N Queen and Curvy Bay, there were two W, made Bay N 
UPDATE miovision_api.centerline_miovision
SET leg = 'N'
WHERE centreline_id = 1145090;

-- 45 N Eglinton and Kingston, two W, so I made Eglinton N and it felt so wrong
UPDATE miovision_api.centerline_miovision
SET leg = 'N'
WHERE centreline_id = 110384;

-- 50 E Kingston and Morningside, two sets of NS, so I made Kingston EW
UPDATE miovision_api.centerline_miovision
SET leg = 'E'
WHERE centreline_id = 107942;

-- 50 W Kingston and Morningside, two sets of NS, so I made Kingston EW
UPDATE miovision_api.centerline_miovision
SET leg = 'W'
WHERE centreline_id = 8087359;

-- 52 N Sheppard and some curvy streets you've never heard of, two W, made the north one N
UPDATE miovision_api.centerline_miovision
SET leg = 'N'
WHERE centreline_id = 440997;

-- 65 W River and Bayview, two S legs so I made River W
UPDATE miovision_api.centerline_miovision
SET leg = 'W'
WHERE centreline_id = 5070833;