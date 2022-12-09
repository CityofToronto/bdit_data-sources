CREATE TABLE IF NOT EXISTS gis.centreline
(
    version_date date,
    centreline_id integer,
    linear_name_id integer,
    linear_name_full text COLLATE pg_catalog."default",
    linear_name_full_legal text COLLATE pg_catalog."default",
    address_l text COLLATE pg_catalog."default",
    address_r text COLLATE pg_catalog."default",
    parity_l text COLLATE pg_catalog."default",
    parity_r text COLLATE pg_catalog."default",
    lo_num_l integer,
    hi_num_l integer,
    lo_num_r integer,
    hi_num_r integer,
    begin_addr_point_id_l integer,
    end_addr_point_id_l integer,
    begin_addr_point_id_r integer,
    end_addr_point_id_r integer,
    begin_addr_l integer,
    end_addr_l integer,
    begin_addr_r integer,
    end_addr_r integer,
    linear_name text COLLATE pg_catalog."default",
    linear_name_type text COLLATE pg_catalog."default",
    linear_name_dir text COLLATE pg_catalog."default",
    linear_name_desc text COLLATE pg_catalog."default",
    linear_name_label text COLLATE pg_catalog."default",
    from_intersection_id integer,
    to_intersection_id integer,
    oneway_dir_code integer,
    oneway_dir_code_desc text COLLATE pg_catalog."default",
    feature_code integer,
    feature_code_desc text COLLATE pg_catalog."default",
    jurisdiction text COLLATE pg_catalog."default",
    centreline_status text COLLATE pg_catalog."default",
    shape_length numeric,
    objectid integer,
    shape_len numeric,
    mi_prinx integer,
    low_num_odd integer,
    high_num_odd integer,
    low_num_even integer,
    high_num_even integer,
    geom geometry
) PARTITION BY LIST (version_date)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS gis.centreline
    OWNER to gis_admins;

GRANT SELECT ON TABLE gis.centreline TO ptc_humans;