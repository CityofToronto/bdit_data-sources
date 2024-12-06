-- Table: congestion_events.itsc_issues

-- DROP TABLE IF EXISTS congestion_events.itsc_issues;

CREATE TABLE IF NOT EXISTS congestion_events.itsc_issues
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
    CONSTRAINT itsc_issues_pkey PRIMARY KEY (divisionid, issueid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS congestion_events.itsc_issues OWNER TO dbadmin;

REVOKE ALL ON TABLE congestion_events.itsc_issues FROM bdit_humans;

GRANT SELECT ON TABLE congestion_events.itsc_issues TO bdit_humans;

GRANT ALL ON TABLE congestion_events.itsc_issues TO dbadmin;

GRANT ALL ON TABLE congestion_events.itsc_issues TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE congestion_events.itsc_issues TO vds_bot;