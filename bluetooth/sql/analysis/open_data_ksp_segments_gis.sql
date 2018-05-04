CREATE VIEW open_data.ksp_bluetooth_gis AS
SELECT segment_name, street, s.direction, start_crossstreet AS from_intersection, 
end_crossstreet AS to_intersection, length, geom
FROM bluetooth.segments s
INNER JOIN king_pilot.bt_segments USING(analysis_id)