-- View: gis_core.centreline_leg_directions

-- DROP TABLE IF EXISTS gis_core.centreline_leg_directions;

CREATE TABLE IF NOT EXISTS gis_core.centreline_leg_directions
(
    intersection_centreline_id integer NOT NULL,
    leg_centreline_id integer NOT NULL,
    leg text,
    intersection_geom geometry,
    street_name text,
    angular_distance real,
    leg_stub_geom geometry,
    leg_full_geom geometry,
    CONSTRAINT centreline_leg_directions_pkey PRIMARY KEY (intersection_centreline_id, leg_centreline_id)
);

ALTER TABLE IF EXISTS gis_core.centreline_leg_directions
OWNER TO gis_admins;

COMMENT ON TABLE gis_core.centreline_leg_directions
IS 'Automated mapping of centreline intersection legs onto the four cardinal directions. Please report any issues/inconsistencies with this view here: https://github.com/CityofToronto/bdit_data-sources/issues/1190';

GRANT SELECT, TRIGGER, REFERENCES ON TABLE gis_core.centreline_leg_directions TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE gis_core.centreline_leg_directions TO gis_admins;
GRANT ALL ON TABLE gis_core.centreline_leg_directions TO rds_superuser WITH GRANT OPTION;

CREATE INDEX centreline_leg_directions_intersection_centreline_id_idx
ON gis_core.centreline_leg_directions USING btree
(intersection_centreline_id)
TABLESPACE pg_default;

CREATE INDEX centreline_leg_directions_leg_full_geom_idx
ON gis_core.centreline_leg_directions USING gist
(leg_full_geom)
TABLESPACE pg_default;

CREATE INDEX centreline_leg_directions_leg_stub_geom_idx
ON gis_core.centreline_leg_directions USING gist
(leg_stub_geom)
TABLESPACE pg_default;

COMMENT ON COLUMN gis_core.centreline_leg_directions.leg
IS 'cardinal direction, one of (north, east, south, west)';

COMMENT ON COLUMN gis_core.centreline_leg_directions.leg_stub_geom
IS 'first (up to) 30m of the centreline segment geometry pointing *inbound* toward the reference intersection';

COMMENT ON COLUMN gis_core.centreline_leg_directions.leg_full_geom
IS 'complete geometry of the centreline segment geometry, pointing *inbound* toward the reference intersection';

COMMENT ON COLUMN gis_core.centreline_leg_directions.angular_offset_from_cardinal_direction
IS 'absolute degrees difference from ideal cardinal direction (oriented to Toronto grid)';
