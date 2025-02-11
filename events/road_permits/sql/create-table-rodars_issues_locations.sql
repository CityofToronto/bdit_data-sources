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
    roadclosuretype integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS congestion_events.rodars_issue_locations OWNER TO congestion_admins;

REVOKE ALL ON TABLE congestion_events.rodars_issue_locations FROM bdit_humans;

GRANT ALL ON TABLE congestion_events.rodars_issue_locations TO dbadmin;

GRANT ALL ON TABLE congestion_events.rodars_issue_locations TO events_bot;

CREATE TRIGGER on_insert_delete_old
AFTER INSERT ON congestion_events.rodars_issue_locations
FOR EACH STATEMENT
EXECUTE FUNCTION congestion_events.delete_old_rodars_issue_locations();

COMMENT ON TABLE congestion_events.rodars_issue_locations IS
'Raw RODARs data. See instead VIEW `congestion_events.rodars_locations`.';
