-- View: gis_core.centreline_latest_all_feature

-- DROP TABLE IF EXISTS gis_core.centreline_latest_all_feature;

CREATE TABLE IF NOT EXISTS gis_core.centreline_latest_all_feature (
    version_date date,
    centreline_id integer NOT NULL,
    linear_name_id integer,
    linear_name_full text,
    linear_name_full_legal text,
    address_l text,
    address_r text,
    parity_l text,
    parity_r text,
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
    linear_name text,
    linear_name_type text,
    linear_name_dir text,
    linear_name_desc text,
    linear_name_label text,
    from_intersection_id integer,
    to_intersection_id integer,
    oneway_dir_code integer,
    oneway_dir_code_desc text,
    feature_code integer,
    feature_code_desc text,
    jurisdiction text,
    centreline_status text,
    shape_length numeric,
    objectid integer,
    shape_len numeric,
    mi_prinx integer,
    low_num_odd integer,
    high_num_odd integer,
    low_num_even integer,
    high_num_even integer,
    geom geometry,
    CONSTRAINT centreline_latest_all_feature_pkey PRIMARY KEY (centreline_id)
);

ALTER TABLE IF EXISTS gis_core.centreline_latest_all_feature
OWNER TO gis_admins;

--comment gets updated on refresh by refresh_centreline_latest_all_feature
COMMENT ON TABLE gis_core.centreline_latest_all_feature
IS 'Table containing the latest version of centreline with all feature code, derived from gis_core.centreline.';

GRANT SELECT ON TABLE gis_core.centreline_latest_all_feature TO bdit_bots;
GRANT SELECT, TRIGGER, REFERENCES ON TABLE gis_core.centreline_latest_all_feature TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE gis_core.centreline_latest_all_feature TO gis_admins;
GRANT ALL ON TABLE gis_core.centreline_latest_all_feature TO rds_superuser WITH GRANT OPTION;

CREATE INDEX gis_core_centreline_latest_all_feature_geom
ON gis_core.centreline_latest_all_feature USING gist
(geom)
TABLESPACE pg_default;
