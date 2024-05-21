CREATE TABLE IF NOT EXISTS vds.centreline_vds
(
    centreline_id bigint NOT NULL,
    vdsconfig_uid integer NOT NULL,
    CONSTRAINT centreline_vds_pkey PRIMARY KEY (vdsconfig_uid),
    CONSTRAINT centreline_vds_vdsconfig_uid_fkey FOREIGN KEY (vdsconfig_uid)
    REFERENCES vds.vdsconfig (uid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS vds.centreline_vds OWNER TO vds_admins;

GRANT ALL ON TABLE vds.centreline_vds TO vds_admins;

REVOKE ALL ON TABLE vds.centreline_vds FROM bdit_humans;
GRANT SELECT ON TABLE vds.centreline_vds TO bdit_humans;

REVOKE ALL ON TABLE vds.centreline_vds FROM vds_bot;
GRANT SELECT ON TABLE vds.centreline_vds TO vds_bot;

COMMENT ON TABLE vds.centreline_vds IS E''
'Use for joining VDS sensors to the centreline. Join using: '
'`vds.centreline_vds LEFT JOIN gis_core.centreline_latest USING (centreline_id)`.';