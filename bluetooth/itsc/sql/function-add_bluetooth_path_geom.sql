CREATE OR REPLACE FUNCTION gwolofs.add_bluetooth_path_geom()
RETURNS TRIGGER AS $$
BEGIN
    -- Add geom by getting the matching paths from centreline_latest
    SELECT st_union(centreline_latest.geom) INTO NEW.geom
    FROM gis_core.centreline_latest
    WHERE centreline_latest.centreline_id = ANY(NEW.centreline_ids); --array of centreline_ids

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION gwolofs.add_bluetooth_path_geom()
OWNER TO gwolofs;
