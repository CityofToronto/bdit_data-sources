CREATE TABLE IF NOT EXISTS gis.centreline_intersection_point
(
    version_date date,
    intersection_id integer,
    date_effective timestamp without time zone,
    date_expiry timestamp without time zone,
    intersection_desc text COLLATE pg_catalog."default",
    ward_number text COLLATE pg_catalog."default",
    ward text COLLATE pg_catalog."default",
    municipality text COLLATE pg_catalog."default",
    classification text COLLATE pg_catalog."default",
    classification_desc text COLLATE pg_catalog."default",
    number_of_elevations integer,
    x numeric,
    y numeric,
    longitude numeric,
    latitude numeric,
    trans_id_create integer,
    trans_id_expire integer,
    objectid integer NOT NULL,
    geom geometry
) PARTITION BY LIST (version_date)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS gis.centreline_intersection_point
    OWNER to gis_admins;

GRANT SELECT ON TABLE gis.centreline_intersection_point TO ptc_humans;