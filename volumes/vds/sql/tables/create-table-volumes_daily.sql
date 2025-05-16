-- Table: vds.volumes_daily

-- DROP TABLE IF EXISTS vds.volumes_daily;

CREATE TABLE IF NOT EXISTS vds.volumes_daily (
    vdsconfig_uid integer,
    entity_location_uid integer,
    detector_id character varying COLLATE pg_catalog."default",
    dt date,
    is_wkdy boolean,
    daily_count bigint,
    count_60day numeric,
    daily_obs bigint,
    daily_obs_expected bigint,
    avg_lanes_present numeric,
    distinct_dt_bins_present bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS vds.volumes_daily OWNER TO vds_admins;

REVOKE ALL ON TABLE vds.volumes_daily FROM bdit_humans;

GRANT REFERENCES, SELECT, TRIGGER ON TABLE vds.volumes_daily TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE vds.volumes_daily TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE vds.volumes_daily TO vds_admins;

COMMENT ON TABLE vds.volumes_daily IS 'A daily view to help with sensor selection.';
