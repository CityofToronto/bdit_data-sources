CREATE TABLE IF NOT EXISTS gis.intersection
(
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
    linear_name_full_from text COLLATE pg_catalog."default",
    linear_name_id_from numeric,
    turn_direction text COLLATE pg_catalog."default",
    centreline_id_to integer,
    linear_name_full_to text COLLATE pg_catalog."default",
    linear_name_id_to numeric,
    connected text COLLATE pg_catalog."default",
    objectid integer,
    elevation_id integer,
    elevation_level integer,
    classification text COLLATE pg_catalog."default",
    classification_desc text COLLATE pg_catalog."default",
    number_of_elevations integer,
    elevation_feature_code integer,
    elevation_feature_code_desc text COLLATE pg_catalog."default",
    elevation numeric,
    elevation_unit text COLLATE pg_catalog."default",
    height_restriction numeric,
    height_restriction_unit text COLLATE pg_catalog."default",
    feature_class_from text COLLATE pg_catalog."default",
    feature_class_to text COLLATE pg_catalog."default",
    geom geometry
) PARTITION BY LIST (version_date)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS gis.intersection
    OWNER to bqu;

GRANT SELECT ON TABLE gis.intersection TO ptc_humans;

GRANT ALL ON TABLE gis.intersection TO gis_admins;