-- Table: congestion_events.rodars_issue_locations

-- DROP TABLE IF EXISTS congestion_events.rodars_issue_locations;

CREATE TABLE IF NOT EXISTS congestion_events.rodars_issue_locations
(
    divisionid smallint NOT NULL,
    issueid integer NOT NULL,
    timestamputc timestamp without time zone,
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
    lanesaffected_direction integer,
    roadname text,
    centreline_id integer,
    linear_name_id integer,
    lanesaffectedpattern text,
    laneblocklevel integer,
    roadclosuretype integer,
    CONSTRAINT itsc_issue_location_pkey PRIMARY KEY (divisionid, issueid, locationindex, direction)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS congestion_events.rodars_issue_locations OWNER TO dbadmin;

REVOKE ALL ON TABLE congestion_events.rodars_issue_locations FROM bdit_humans;
GRANT SELECT ON TABLE congestion_events.rodars_issue_locations TO bdit_humans;

GRANT ALL ON TABLE congestion_events.rodars_issue_locations TO dbadmin;

GRANT ALL ON TABLE congestion_events.rodars_issue_locations TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE congestion_events.rodars_issue_locations TO events_bot;

CREATE TRIGGER on_insert_delete_old
AFTER INSERT ON congestion_events.rodars_issue_locations
FOR EACH STATEMENT
EXECUTE FUNCTION congestion_events.delete_old_rodars_issue_locations();
