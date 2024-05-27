--this query may help when updating vds.centrline_vds table.

DROP MATERIALIZED VIEW IF EXISTS gwolofs.vds_centreline_temp;
CREATE MATERIALIZED VIEW gwolofs.vds_centreline_temp AS (

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
            --v.direction, --this was only for rescu detectors, not boradly applicable
            UPPER(e.main_road_name) || ' and ' || UPPER(e.cross_road_name) AS detector_loc,
            i.sensor_geom,
            e.main_road_id AS linear_name_id
        FROM vds.detector_inventory AS i
        LEFT JOIN vds.entity_locations AS e ON e.uid = i.entity_location_uid
        LEFT JOIN vds.vdsconfig AS v ON v.uid = i.vdsconfig_uid
        --fitler here
        WHERE i.centreline_id IS NULL
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
);

--look at the results, using QGIS to plot both sensor_geom and cl_geom at once.
SELECT * FROM gwolofs.vds_centreline_temp; --noqa: L044

INSERT INTO vds.centreline_vds (centreline_id, vdsconfig_uid)
SELECT
    centreline_id,
    vdsconfig_uid
FROM gwolofs.vds_centreline_temp
WHERE centreline_id IS NOT NULL;

--when you are done examining:
DROP MATERIALIZED VIEW gwolofs.vds_centreline_temp;