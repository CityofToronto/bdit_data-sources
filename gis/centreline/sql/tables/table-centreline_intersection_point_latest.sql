-- View: gis_core.centreline_intersection_point_latest

-- DROP TABLE IF EXISTS gis_core.centreline_intersection_point_latest;

CREATE TABLE IF NOT EXISTS gis_core.centreline_intersection_point_latest (
    version_date date,
    intersection_id integer NOT NULL,
    date_effective timestamp without time zone,
    date_expiry timestamp without time zone,
    intersection_desc text,
    ward_number text,
    ward text,
    municipality text,
    classification text,
    classification_desc text,
    number_of_elevations integer,
    x numeric,
    y numeric,
    longitude numeric,
    latitude numeric,
    trans_id_create integer,
    trans_id_expire integer,
    objectid integer,
    geom geometry,
    CONSTRAINT centreline_intersection_point_latest_pkey PRIMARY KEY (intersection_id)
)

ALTER TABLE IF EXISTS gis_core.centreline_intersection_point_latest
OWNER TO gis_admins;

COMMENT ON TABLE gis_core.centreline_intersection_point_latest
IS 'Table containing the latest version of centreline intersection point,derived from gis_core.centreline_intersection_point. Removes some (rare) duplicate intersection_ids.';

GRANT SELECT ON TABLE gis_core.centreline_intersection_point_latest TO bdit_bots;
GRANT SELECT, TRIGGER, REFERENCES ON TABLE gis_core.centreline_intersection_point_latest TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE gis_core.centreline_intersection_point_latest TO gis_admins;
GRANT ALL ON TABLE gis_core.centreline_intersection_point_latest TO rds_superuser WITH GRANT OPTION;

CREATE INDEX gis_core_centreline_intersection_point_latest_geom
ON gis_core.centreline_intersection_point_latest USING gist
(geom)
TABLESPACE pg_default;

CREATE INDEX gis_core_centreline_intersection_point_latest_geom_m
ON gis_core.centreline_intersection_point_latest USING gist
(st_transform(geom, 2952))
TABLESPACE pg_default;