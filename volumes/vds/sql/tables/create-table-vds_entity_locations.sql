CREATE TABLE IF NOT EXISTS vds.entity_locations (
    uid serial PRIMARY KEY,
    division_id smallint,
    entity_type smallint,
    entity_id integer,
    start_timestamp timestamp without time zone,
    end_timestamp timestamp without time zone,
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
    geom geometry,
    UNIQUE (division_id, entity_id, start_timestamp)
);

ALTER TABLE vds.entity_locations OWNER TO vds_admins;
GRANT INSERT, SELECT, UPDATE ON TABLE vds.entity_locations TO vds_bot;
GRANT ALL ON SEQUENCE vds.entity_locations_uid_seq TO vds_bot;
GRANT SELECT ON TABLE vds.entity_locations TO bdit_humans;

COMMENT ON TABLE vds.entity_locations IS 'Store raw data pulled from ITS Central `entitylocations` 
table. Note entity_locations.entity_id corresponds to vdsconfig.vds_id. Also note there are
duplicates on entity_id corresponding to updated locations over time.';

-- DROP INDEX IF EXISTS vds.ix_entity_locations_full;
CREATE INDEX IF NOT EXISTS ix_entity_locations_full
ON vds.entity_locations
USING btree (
    division_id ASC NULLS LAST,
    entity_id ASC NULLS LAST,
    start_timestamp ASC NULLS LAST,
    end_timestamp ASC NULLS LAST
);
