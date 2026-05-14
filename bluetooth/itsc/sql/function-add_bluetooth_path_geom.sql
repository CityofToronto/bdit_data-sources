CREATE OR REPLACE FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
RETURNS trigger AS $$
BEGIN
    WITH ordered_ids AS (
        SELECT ordinality, centreline_id
        -- Unnest the array of centreline_ids with ordinality to preserve order
        FROM unnest(NEW.centreline_ids) WITH ORDINALITY AS t (centreline_id, ordinality)
    ),
    
    cent AS (
        -- Add geom by getting the paths from centreline (sort by version_id)
        SELECT DISTINCT ON (c.centreline_id)
            c.centreline_id,
            c.geom,
            o.ordinality
        FROM gis_core.centreline AS c
        JOIN ordered_ids AS o USING (centreline_id)
        ORDER BY
            c.centreline_id,
            c.version_date DESC
    )
    
    SELECT ST_LineMerge(ST_Collect(geom ORDER BY ordinality)) INTO NEW.geom
    FROM cent;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
OWNER TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.itsc_add_bluetooth_path_geom TO events_bot;
