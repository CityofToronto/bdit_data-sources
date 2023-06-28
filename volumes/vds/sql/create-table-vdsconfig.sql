--DROP TABLE vds.vdsconfig; 

CREATE TABLE vds.vdsconfig (
    divisionid smallint,
    vdsid integer,
    detector_id character varying,
    starttimestamputc timestamp without time zone,
    endtimestamputc timestamp without time zone,
    lanes smallint,
    hasgpsunit boolean,
    managementurl character varying,
    description character varying,
    fssdivisionid integer,
    fssid integer,
    rtmsfromzone integer,
    rtmstozone integer,
    detectortype smallint,
    createdby character varying,
    createdbystaffid uuid,
    signalid integer,
    signaldivisionid smallint,
    movement smallint,
    PRIMARY KEY (divisionid, vdsid, starttimestamputc)
);

ALTER TABLE vds.vdsconfig OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.vdsconfig TO vds_bot;