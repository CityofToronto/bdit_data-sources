--temporarily drop trigger
DROP TRIGGER IF EXISTS add_bluetooth_path_geom_trigger ON bluetooth.itsc_tt_paths;

--use this script to initially populated itsc_tt_paths.geom column, or for mass updates.
UPDATE bluetooth.itsc_tt_paths
SET
    geom := ST_LineFromEncodedPolyline(encoded_polyline),
    start_node := st_startpoint(ST_LineFromEncodedPolyline(encoded_polyline)),
    end_node := st_endpoint(ST_LineFromEncodedPolyline(encoded_polyline));

--readd geom update trigger
CREATE OR REPLACE TRIGGER add_bluetooth_path_geom_trigger
    BEFORE INSERT OR UPDATE 
    ON bluetooth.itsc_tt_paths
    FOR EACH ROW
    EXECUTE FUNCTION bluetooth.itsc_add_bluetooth_path_geom();
