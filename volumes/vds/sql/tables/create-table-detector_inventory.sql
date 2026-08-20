CREATE TABLE gwolofs.detector_inventory (
    row_id integer NOT NULL DEFAULT nextval('gwolofs.detector_inventory_row_id_seq'::regclass),
    vdsconfig_uid integer,
    entity_location_uid integer,
    division_id smallint,
    detector_id character varying,
    first_active timestamp without time zone,
    last_active timestamp without time zone,
    detector_loc text,
    sensor_geom geometry,
    centreline_id bigint,
    centreline_geom geometry,
    det_type text,
    det_loc text,
    det_group text,
    direction text,
    expected_bins integer,
    comms_desc character varying(5000),
    det_tech text,
    CONSTRAINT detector_inventory_vdsconfig_uid_entity_location_uid_divisi_key UNIQUE (vdsconfig_uid, entity_location_uid, division_id)
);

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.detector_inventory OWNER TO vds_admins;

REVOKE ALL ON TABLE gwolofs.detector_inventory FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.detector_inventory TO bdit_humans;

COMMENT ON TABLE gwolofs.detector_inventory IS 'Updated daily by vds_pull_vdsdata DAG.
Pulls data from many VDS tables to simplify sensor selection/id.
May need to periodically update `expected_bins` column using bdit_data-sources/volumes/vds/exploration/time_gaps.sql';
