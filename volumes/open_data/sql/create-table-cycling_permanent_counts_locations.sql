-- Table: open_data.cycling_permanent_counts_locations

-- DROP TABLE IF EXISTS open_data.cycling_permanent_counts_locations;

CREATE TABLE IF NOT EXISTS open_data.cycling_permanent_counts_locations
(
    location_dir_id bigint NOT NULL DEFAULT nextval(
        'open_data.cycling_permanent_counts_location_dir_id'::regclass
    ),
    location_name text COLLATE pg_catalog."default",
    direction text COLLATE pg_catalog."default",
    linear_name_full text COLLATE pg_catalog."default",
    side_street text COLLATE pg_catalog."default",
    longitude numeric,
    latitude numeric,
    centreline_id integer,
    bin_size text COLLATE pg_catalog."default",
    latest_calibration_study date,
    first_active date,
    last_active date,
    date_decommissioned date,
    technology text COLLATE pg_catalog."default",
    CONSTRAINT cycling_permanent_counts_locations_pkey PRIMARY KEY (
        location_dir_id
    ),
    CONSTRAINT unique_cycling_permanent_counts_locations UNIQUE (
        location_name, direction
    )
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS open_data.cycling_permanent_counts_locations
OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.cycling_permanent_counts_locations FROM bdit_humans;
REVOKE ALL ON TABLE open_data.cycling_permanent_counts_locations FROM ecocounter_bot;

GRANT SELECT ON TABLE open_data.cycling_permanent_counts_locations TO bdit_humans;

GRANT UPDATE, INSERT, SELECT ON TABLE open_data.cycling_permanent_counts_locations
TO ecocounter_bot;

GRANT ALL ON TABLE open_data.cycling_permanent_counts_locations TO od_admins;

GRANT ALL ON TABLE open_data.cycling_permanent_counts_locations TO rds_superuser WITH GRANT OPTION;
