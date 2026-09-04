---- New Table to isolate the new sites ---

-- Drop if exists
DROP TABLE IF EXISTS ecocounter_new_sites;

-- Drop if exists
DROP TABLE IF EXISTS temp_ecocounter_centrelines;

CREATE TEMP TABLE ecocounter_new_sites AS (
    SELECT * FROM ecocounter.sites_unfiltered
    WHERE
        site_id IN
        (
            SELECT t.site_id
            FROM temp_ecocounter_changes AS t
            WHERE t.change = 'new_site'
        )
);

CREATE TEMP TABLE temp_ecocounter_centrelines AS (
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
        rank() OVER (
            ORDER BY det.site_id
        ) AS _rank, --uid needed for plotting in qgis
        det.site_id,
        cl.centreline_id,
        cl.linear_name_full,
        cl.geom AS centreline_geom,
        cl.feature_code_desc,
        det.site_description AS detector_loc,
        det.geom AS sensor_geom
    FROM ecocounter_new_sites AS det
    LEFT JOIN centrelines AS cl
        ON st_intersects(cl.geom, st_buffer(det.geom, 0.01))
    WHERE det.centreline_id IS NULL
    ORDER BY
        det.site_id,
        --select the closest match
        st_distance(det.geom, cl.geom)
);

-- Did a visual check in QGIS -looks  (almost) good!
UPDATE temp_ecocounter_centrelines
SET
	centreline_id = 20040975,
	linear_name_full = 'Sheppard Ave E'
WHERE site_id = 300066351;

-- Apply changes 
UPDATE ecocounter.sites_unfiltered AS a
SET
    centreline_id = b.centreline_id,
    linear_name_full = b.linear_name_full,
    technology = 'Induction - Eco-Counter',
    side_street = TRIM(SPLIT_PART(site_description, ' of ', 2))
FROM temp_ecocounter_centrelines AS b
WHERE a.site_id = b.site_id;

UPDATE ecocounter.sites_unfiltered AS a
SET 
    side_street = REGEXP_REPLACE(side_street, '\([^)]*\)', '')
FROM temp_ecocounter_centrelines AS b
WHERE a.site_id = b.site_id;

-- See
--SELECT * FROM dboucau.sites_unfiltered
--		WHERE site_id IN
--		(SELECT site_id 
--			FROM temp_ecocounter_changes
--			WHERE change = 'new_site');

