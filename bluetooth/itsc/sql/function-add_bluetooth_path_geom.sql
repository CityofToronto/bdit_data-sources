CREATE OR REPLACE FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
RETURNS trigger AS $$
BEGIN

    NEW.geom := ST_SetSRID(ST_LineFromEncodedPolyline(NEW.encoded_polyline), 4326);
    NEW.start_node := ST_SetSRID(st_startpoint(ST_LineFromEncodedPolyline(NEW.encoded_polyline)), 4326);
    NEW.end_node := ST_SetSRID(st_endpoint(ST_LineFromEncodedPolyline(NEW.encoded_polyline)), 4326);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bluetooth.itsc_add_bluetooth_path_geom()
OWNER TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.itsc_add_bluetooth_path_geom TO events_bot;
