--DROP TABLE vds.entity_locations; 

CREATE TABLE vds.entity_locations (
    uid serial, 
    division_id smallint,
    entity_type smallint,
    entity_id integer,
    location_timestamp timestamp without time zone,
    latitude double precision,
    longitude double precision,
    altitude_meters_asl double precision,
    heading_degrees double precision,
    speed_kmh double precision,
    num_satellites integer,
    dilution_of_precision double precision,
    main_road_id integer,
    cross_road_id integer,
    second_cross_road_id integer,
    main_road_name character varying,
    cross_road_name character varying,
    second_cross_road_name character varying,
    street_number character varying,
    offset_distance_meters double precision,
    offset_direction_degrees double precision,
    location_source smallint,
    location_description_overwrite character varying,
    PRIMARY KEY uid, 
    UNIQUE(division_id, entity_id, location_timestamp)
);

ALTER TABLE vds.entity_locations OWNER TO vds_admins;
GRANT INSERT, SELECT ON TABLE vds.entity_locations TO vds_bot;