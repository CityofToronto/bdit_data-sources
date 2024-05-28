--use a temp table to visualize ecocounter - centreline intersection matches. 

CREATE TEMP TABLE ecocounter_centrelines AS (
    WITH centrelines AS (
            SELECT
                centreline_id,
                linear_name_full,
                ST_SetSRID(geom, 4326) AS geom,
                feature_code_desc
            FROM gis_core.centreline_latest
            WHERE feature_code_desc NOT IN (
                'Expressway',
                'Expressway Ramp'
            ) -- definitely no ecocounters here!
        )
    
    SELECT DISTINCT ON (det.site_id)
        rank() OVER (ORDER BY det.site_id) AS _rank, --uid needed for plotting in qgis
        det.site_id,
        cl.centreline_id,
        cl.linear_name_full,
        cl.geom AS centreline_geom,
        cl.feature_code_desc,
        det.site_description AS detector_loc,
        det.geom AS sensor_geom
    FROM ecocounter.sites AS det
    LEFT JOIN centrelines AS cl
        ON st_intersects(cl.geom, st_buffer(det.geom, 0.01))
    WHERE det.centreline_id IS NULL
    ORDER BY
        det.site_id,
        --select the closest match
        st_distance(det.geom, cl.geom)
);

--can visualize with two layers in qgis to see both centreline and sensor geom at once. 
SELECT * FROM ecocounter_centrelines;

--when satisfied, update sites_unfiltered:
UPDATE ecocounter.sites_unfiltered
SET centreline_id = cl.centreline_id
FROM ecocounter_centrelines AS cl
WHERE
    sites_unfiltered.site_id = cl.site_id
    AND sites_unfiltered.centreline_id IS NULL;