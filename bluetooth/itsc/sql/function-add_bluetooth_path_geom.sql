CREATE OR REPLACE FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
RETURNS TRIGGER AS $$
BEGIN
    -- Add geom by getting the matching paths from centreline_latest
    WITH cent AS (
        SELECT DISTINCT ON (centreline_id) centreline_id, geom
        FROM gis_core.centreline
        WHERE centreline_id = ANY(NEW.centreline_ids) --array of centreline_ids
        ORDER BY centreline_id, version_date DESC
    )
    SELECT st_union(geom) INTO NEW.geom
    FROM cent;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
OWNER TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.itsc_add_bluetooth_path_geom TO events_bot; 
