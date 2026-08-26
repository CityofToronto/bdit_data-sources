CREATE OR REPLACE FUNCTION vds.refresh_centrelines()
RETURNS VOID AS $$

--if the centreline has fallen out of centreline_latest, delete and reprocess.
DELETE FROM vds.centreline_vds
WHERE
    centreline_id IS NOT NULL --allow for null placeholders
    AND centreline_id NOT IN (
        SELECT centreline_id FROM gis_core.centreline_latest
    );

/*
--when we change to detector_inventory as a table, we will need to cascade updates manually (or with a trigger).
UPDATE vds.detector_inventory
SET centreline_id = NULL, centreline_geom = NULL
WHERE ...
*/

WITH vds_centreline_temp AS (

    -- get centreline segments
    WITH centrelines AS (
        SELECT
            centreline_id,
            linear_name_full AS cent_name,
            ST_SetSRID(geom, 4326) AS geom,
            feature_code_desc,
            --not as detailed as centreline_id, but still useful, ie. "Lakeshore Blvd W"
            linear_name_id
        FROM gis_core.centreline_latest
        WHERE feature_code_desc IN (
            'Major Arterial',
            'Major Arterial Ramp',
            'Expressway',
            'Expressway Ramp'
        ) -- these are the only types of roads we need
    ),

    -- get RESCU detectors that pass the "good volume" tests
    detectors AS (
        SELECT
            i.vdsconfig_uid,
            i.detector_id,
            UPPER(e.main_road_name) || ' and ' || UPPER(e.cross_road_name) AS detector_loc,
            i.sensor_geom,
            e.main_road_id AS linear_name_id
        FROM vds.detector_inventory AS i
        LEFT JOIN vds.entity_locations AS e ON e.uid = i.entity_location_uid
        LEFT JOIN vds.vdsconfig AS v ON v.uid = i.vdsconfig_uid
        WHERE
            (
                i.centreline_id IS NULL
                OR i.centreline_geom IS NULL
            ) AND i.division_id = 2
    )

    -- spatially join buffered detectors and segments
    SELECT DISTINCT ON (det.vdsconfig_uid)
        rank() OVER (ORDER BY det.vdsconfig_uid) AS _rank, --uid needed for plotting in qgis
        det.vdsconfig_uid,
        cl.centreline_id,
        cl.cent_name,
        cl.geom AS centreline_geom,
        cl.feature_code_desc,
        cl.linear_name_id,
        det.detector_id,
        det.detector_loc,
        det.sensor_geom
    FROM detectors AS det
    LEFT JOIN centrelines AS cl
    --with this we can be confident we aren't matching to the wrong road!
        --Field does not appear to always be populated.
        ON cl.linear_name_id = det.linear_name_id
        --increased tolerance due to addition of linear_name_id
        AND st_intersects(cl.geom, st_buffer(det.sensor_geom, 0.01))
    ORDER BY
        det.vdsconfig_uid,
        --select the closest match
        st_distance(det.sensor_geom, cl.geom)
)

INSERT INTO vds.centreline_vds (centreline_id, vdsconfig_uid)
SELECT
    centreline_id,
    vdsconfig_uid
FROM vds_centreline_temp
--we may use null centrelines as a placeholder for when centrelines don't exist, but we shouldn't insert them automatically
WHERE centreline_id IS NOT NULL;

$$
LANGUAGE sql
SECURITY DEFINER;

ALTER FUNCTION vds.refresh_centrelines() OWNER TO vds_admins;
GRANT EXECUTE ON FUNCTION vds.refresh_centrelines() TO vds_bot;
REVOKE EXECUTE ON FUNCTION vds.refresh_centrelines() FROM bdit_humans; 
