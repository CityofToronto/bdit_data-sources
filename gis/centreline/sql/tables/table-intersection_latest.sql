-- View: gis_core.intersection_latest

-- DROP TABLE IF EXISTS gis_core.intersection_latest;

CREATE TABLE IF NOT EXISTS gis_core.intersection_latest(
    version_date date,
    intersection_id integer,
    date_effective timestamp without time zone,
    date_expiry timestamp without time zone,
    trans_id_create integer,
    trans_id_expire integer,
    x numeric,
    y numeric,
    longitude numeric,
    latitude numeric,
    centreline_id_from integer,
    linear_name_full_from text,
    linear_name_id_from numeric,
    turn_direction text,
    centreline_id_to integer,
    linear_name_full_to text,
    linear_name_id_to numeric,
    connected text,
    objectid integer NOT NULL,
    elevation_id integer,
    elevation_level integer,
    classification text,
    classification_desc text,
    number_of_elevations integer,
    elevation_feature_code integer,
    elevation_feature_code_desc text,
    elevation numeric,
    elevation_unit text,
    height_restriction numeric,
    height_restriction_unit text,
    feature_class_from text,
    feature_class_to text,
    geom geometry,
    CONSTRAINT intersection_latest_pkey PRIMARY KEY (objectid)
)
ALTER TABLE IF EXISTS gis_core.intersection_latest
OWNER TO gis_admins;

--comment gets updated on refresh by refresh_intersection_latest
COMMENT ON TABLE gis_core.intersection_latest
IS 'Table containing the latest version of intersection , derived from gis_core.intersection.';

GRANT INSERT, UPDATE ON TABLE gis_core.intersection_latest TO bdit_humans;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE gis_core.intersection_latest TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE gis_core.intersection_latest TO gis_admins;
GRANT ALL ON TABLE gis_core.intersection_latest TO rds_superuser WITH GRANT OPTION;
GRANT SELECT ON TABLE gis_core.intersection_latest TO tt_request_bot;

CREATE INDEX gis_core_intersection_latest_geom
ON gis_core.intersection_latest USING gist
(geom)
TABLESPACE pg_default;
