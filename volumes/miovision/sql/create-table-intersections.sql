CREATE TABLE miovision_api.intersections_new
(
    intersection_uid integer,
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
    geom geometry(Point,4326),
    n_leg_restricted boolean,
    e_leg_restricted boolean,
    s_leg_restricted boolean,
    w_leg_restricted boolean
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;