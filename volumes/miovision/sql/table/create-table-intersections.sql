CREATE TABLE miovision_api.intersections
(
    intersection_uid integer NOT NULL DEFAULT nextval('miovision_api.miovision_intersection_uids'::regclass),
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
    api_name text COLLATE pg_catalog."default",
    CONSTRAINT intersections_pkey PRIMARY KEY (intersection_uid),
    CONSTRAINT miovision_intersections_id_uniq UNIQUE (id)
)
WITH (
    oids = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.intersections
OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.intersections FROM bdit_humans;
GRANT SELECT ON TABLE miovision_api.intersections TO bdit_humans;

GRANT ALL ON TABLE miovision_api.intersections TO miovision_admins;

GRANT ALL ON TABLE miovision_api.intersections TO miovision_api_bot;

COMMENT ON COLUMN miovision_api.intersections.intersection_name
IS 'A short name for the intersection, following the convention
[E / W street name] / [N / S street name].';

COMMENT ON COLUMN miovision_api.intersections.api_name
IS 'The intersection name used in the Miovision API, for communication with external parties.';
