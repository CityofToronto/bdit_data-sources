CREATE OR REPLACE FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
RETURNS trigger AS $$
BEGIN

    NEW.geom := ST_LineFromEncodedPolyline(NEW.encoded_polyline);
    NEW.start_node := st_startpoint(ST_LineFromEncodedPolyline(NEW.encoded_polyline));
    NEW.end_node := st_endpoint(ST_LineFromEncodedPolyline(NEW.encoded_polyline));

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
OWNER TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.itsc_add_bluetooth_path_geom TO events_bot;
