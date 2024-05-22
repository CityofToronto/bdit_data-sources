-- Table: vds.vdsconfig_x_entity_locations

-- DROP TABLE IF EXISTS vds.vdsconfig_x_entity_locations;

CREATE TABLE IF NOT EXISTS vds.vdsconfig_x_entity_locations
(
    detector_uid integer NOT NULL DEFAULT nextval('vds.vds_pairs_detector_uid_seq'::regclass),
    division_id smallint NOT NULL,
    vdsconfig_uid integer NOT NULL,
    entity_location_uid integer NOT NULL,
    first_active timestamp without time zone,
    last_active timestamp without time zone,
    CONSTRAINT vds_pairings_pkey PRIMARY KEY (detector_uid),
    CONSTRAINT vds_pairings_unique UNIQUE (vdsconfig_uid, entity_location_uid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS vds.vdsconfig_x_entity_locations
OWNER TO vds_admins;

REVOKE ALL ON TABLE vds.vdsconfig_x_entity_locations FROM bdit_humans;
REVOKE ALL ON TABLE vds.vdsconfig_x_entity_locations FROM vds_bot;

GRANT SELECT ON TABLE vds.vdsconfig_x_entity_locations TO bdit_humans;

GRANT ALL ON TABLE vds.vdsconfig_x_entity_locations TO vds_admins;

GRANT SELECT, UPDATE, INSERT ON TABLE vds.vdsconfig_x_entity_locations TO vds_bot;

COMMENT ON TABLE vds.vdsconfig_x_entity_locations
IS 'All combinations of vdsconfig_uid x entity_location_uid which appear in counts_15min data.';