CREATE TABLE miovision_api.intersections
(
    intersection_uid integer PRIMARY KEY,
    id text COLLATE pg_catalog."default",
    intersection_name text COLLATE pg_catalog."default",
    date_installed date,
    date_decommissioned date,
    lat numeric,
    lng numeric,
    street_main text COLLATE pg_catalog."default",
    street_cross text COLLATE pg_catalog."default",
    int_id bigint,
    px integer,
    geom geometry (POINT, 4326),
    n_leg_restricted boolean,
    e_leg_restricted boolean,
    s_leg_restricted boolean,
    w_leg_restricted boolean,
    api_name text COLLATE pg_catalog."default"
)
WITH (
    oids = FALSE
)
TABLESPACE pg_default;

COMMENT ON COLUMN miovision_api.intersections.intersection_name
IS 'A short name for the intersection, following the convention
[E / W street name] / [N / S street name].';

COMMENT ON COLUMN miovision_api.intersections.api_name
IS 'The intersection name used in the Miovision API, for communication with external parties.';
