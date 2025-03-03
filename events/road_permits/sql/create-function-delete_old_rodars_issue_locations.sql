CREATE OR REPLACE FUNCTION congestion_events.delete_old_rodars_issue_locations()
RETURNS trigger AS $$
BEGIN

    WITH latest AS (
        SELECT
            divisionid,
            issueid,
            MAX(timestamputc) AS max_timestamputc
        FROM congestion_events.rodars_issue_locations
        GROUP BY
            divisionid,
            issueid
    )

  -- Delete records older than the current one for the same primary keys
  DELETE FROM congestion_events.rodars_issue_locations AS iil
  USING latest
  WHERE
      iil.divisionid = latest.divisionid
      AND iil.issueid = latest.issueid
      AND iil.timestamputc < latest.max_timestamputc;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION congestion_events.delete_old_rodars_issue_locations TO congestion_admins;
GRANT EXECUTE ON FUNCTION congestion_events.delete_old_rodars_issue_locations TO events_bot;

COMMENT ON FUNCTION congestion_events.delete_old_rodars_issue_locations IS
'Deletes old records from congestion_events.rodars_issue_locations on insert (trigger).';