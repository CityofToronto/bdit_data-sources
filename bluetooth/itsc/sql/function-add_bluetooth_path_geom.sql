CREATE OR REPLACE FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
RETURNS TRIGGER AS $$
BEGIN
    -- Add geom by getting the matching paths from centreline_latest
    SELECT st_union(centreline_latest.geom) INTO NEW.geom
    FROM gis_core.centreline_latest
    WHERE centreline_latest.centreline_id = ANY(NEW.centreline_ids); --array of centreline_ids

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
OWNER TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.itsc_add_bluetooth_path_geom TO events_bot; 
