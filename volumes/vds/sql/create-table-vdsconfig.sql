--DROP TABLE gwolofs.vdsconfig; 

CREATE TABLE gwolofs.vdsconfig (
    divisionid smallint,
    vdsid integer,
    sourceid character varying,
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

--ALTER TABLE gwolofs.vdsconfig OWNER TO rescu_admins;