--DROP TABLE vds.vdsconfig; 

CREATE TABLE IF NOT EXISTS vds.vdsconfig (
    uid serial PRIMARY KEY,
    division_id smallint,
    vds_id integer,
    detector_id character varying,
    start_timestamp timestamp without time zone,
    end_timestamp timestamp without time zone,
    lanes smallint,
    has_gps_unit boolean,
    management_url character varying,
    description character varying,
    fss_division_id integer,
    fss_id integer,
    rtms_from_zone integer,
    rtms_to_zone integer,
    detector_type smallint,
    created_by character varying,
    created_by_staffid uuid,
    signal_id integer,
    signal_division_id smallint,
    movement smallint,
    UNIQUE (division_id, vds_id, start_timestamp)
);

ALTER TABLE vds.vdsconfig OWNER TO vds_admins;
GRANT INSERT, SELECT, UPDATE ON TABLE vds.vdsconfig TO vds_bot;
GRANT ALL ON SEQUENCE vds.vdsconfig_uid_seq TO vds_bot;
GRANT SELECT ON TABLE vds.vdsconfig TO bdit_humans;

COMMENT ON TABLE vds.vdsconfig IS 'Store raw data pulled from ITS Central `vdsconfig` table.
Note there are duplicates on vds_id corresponding to updated locations/details over time.';

-- DROP INDEX IF EXISTS vds.ix_entity_locations_full;
CREATE INDEX IF NOT EXISTS ix_vdsconfig_full
ON vds.vdsconfig
USING btree(
    division_id ASC nulls last,
    vds_id ASC nulls last, -- noqa: PRS
    start_timestamp ASC nulls last,
    end_timestamp ASC nulls last
);
