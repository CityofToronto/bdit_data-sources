--DROP TABLE vds.vdsconfig; 

CREATE TABLE vds.vdsconfig (
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
    PRIMARY KEY (division_id, vds_id, start_timestamp)
);

ALTER TABLE vds.vdsconfig OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.vdsconfig TO vds_bot;