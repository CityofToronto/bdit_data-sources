-- Table: gwolofs.rodars_new

-- DROP TABLE IF EXISTS gwolofs.rodars_new;

CREATE TABLE IF NOT EXISTS gwolofs.rodars_new
(
    divisionid smallint NOT NULL,
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
    CONSTRAINT rodars_new_pkey PRIMARY KEY (divisionid, issueid, locationindex)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.rodars_new OWNER TO dbadmin;

REVOKE ALL ON TABLE gwolofs.rodars_new FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.rodars_new TO bdit_humans;

GRANT ALL ON TABLE gwolofs.rodars_new TO dbadmin;

GRANT ALL ON TABLE gwolofs.rodars_new TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE gwolofs.rodars_new TO vds_bot;