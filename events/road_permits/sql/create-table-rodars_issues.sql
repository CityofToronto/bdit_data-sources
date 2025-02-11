-- Table: congestion_events.rodars_issues

-- DROP TABLE IF EXISTS congestion_events.rodars_issues;

CREATE TABLE IF NOT EXISTS congestion_events.rodars_issues
(
    divisionid smallint NOT NULL,
    divisionname text,
    issueid integer NOT NULL,
    timestamputc timestamp without time zone,
    issuetype smallint,
    description text,
    priority smallint,
    proposedstarttimestamp timestamp without time zone,
    proposedendtimestamp timestamp without time zone,
    earlyendtimestamp timestamp without time zone,
    status integer,
    timeoption smallint,
    sourceid text,
    starttimestamp timestamp without time zone,
    endtimestamp timestamp without time zone,
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
    CONSTRAINT rodars_issues_pkey PRIMARY KEY (divisionid, issueid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS congestion_events.rodars_issues OWNER TO congestion_admins;

REVOKE ALL ON TABLE congestion_events.rodars_issues FROM bdit_humans;

GRANT ALL ON TABLE congestion_events.rodars_issues TO dbadmin;

GRANT ALL ON TABLE congestion_events.rodars_issues TO events_bot;
