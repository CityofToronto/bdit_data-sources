-- Table: gwolofs.itsc_issues

-- DROP TABLE IF EXISTS gwolofs.itsc_issues;

CREATE TABLE IF NOT EXISTS gwolofs.itsc_issues
(
    divisionid smallint NOT NULL,
    divisionname text,
    issueid integer NOT NULL,
    timestamputc timestamp without time zone,
    issuetype smallint,
    description text,
    priority smallint,
    proposedstarttimestamputc timestamp without time zone,
    proposedendtimestamputc timestamp without time zone,
    earlyendtimestamputc timestamp without time zone,
    status integer,
    timeoption smallint,
    locationindex integer,
    mainroadname text,
    fromroadname text,
    toroadname text,
    direction smallint,
    lanesaffected text,
    geometry geometry,
    streetnumber text,
    locationtype integer,
    groupid integer,
    groupdescription text,
    sourceid text,
    starttimestamputc timestamp without time zone,
    endtimestamputc timestamp without time zone,
    kmpost double precision,
    managementurl text,
    cancellationstatus integer,
    closeissueonplannedendtime boolean,
    plannedstartadvancenoticeseconds integer,
    plannedendadvancenoticeseconds integer,
    locationdescriptionoverwrite text,
    startissueonplannedstarttime boolean,
    startstatus integer,
    updateremindernoticeseconds integer,
    CONSTRAINT itsc_issues_pkey PRIMARY KEY (divisionid, issueid, locationindex)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.itsc_issues OWNER TO dbadmin;

REVOKE ALL ON TABLE gwolofs.itsc_issues FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.itsc_issues TO bdit_humans;

GRANT ALL ON TABLE gwolofs.itsc_issues TO dbadmin;

GRANT ALL ON TABLE gwolofs.itsc_issues TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE gwolofs.itsc_issues TO vds_bot;