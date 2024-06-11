INSERT INTO vds.entity_locations AS e (
    division_id, entity_type, entity_id, start_timestamp, end_timestamp, latitude, longitude, altitude_meters_asl, 
    heading_degrees, speed_kmh, num_satellites, dilution_of_precision, main_road_id, cross_road_id,
    second_cross_road_id, main_road_name, cross_road_name, second_cross_road_name, street_number,
    offset_distance_meters, offset_direction_degrees, location_source, location_description_overwrite)
VALUES %s
ON CONFLICT (division_id, entity_id, start_timestamp)
DO UPDATE
SET end_timestamp = EXCLUDED.end_timestamp
WHERE
    e.division_id = EXCLUDED.division_id
    AND e.entity_id = EXCLUDED.entity_id
    AND e.start_timestamp = EXCLUDED.start_timestamp;

--add geom to new rows
UPDATE vds.entity_locations
SET geom = ST_SetSRID(ST_MAKEPOINT(longitude, latitude), 4326)
WHERE geom IS NULL;