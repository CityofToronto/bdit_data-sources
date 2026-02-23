-- FUNCTION: ecocounter.if_modified_func()

-- DROP FUNCTION IF EXISTS ecocounter.if_modified_func();

CREATE OR REPLACE FUNCTION ecocounter.if_modified_func()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
    SET search_path=pg_catalog, public
AS $BODY$
DECLARE
    audit_row ecocounter.logged_actions;
    include_values boolean;
    log_diffs boolean;
    h_old jsonb;
    h_new jsonb;
    excluded_cols text[] = ARRAY[]::text[];
BEGIN
    IF TG_WHEN <> 'AFTER' THEN
        RAISE EXCEPTION 'ecocounter.if_modified_func() may only run as an AFTER trigger';
    END IF;

    audit_row = ROW(
        nextval('ecocounter.logged_actions_event_id_seq'), -- event_id
        TG_TABLE_SCHEMA::text,                        -- schema_name
        TG_TABLE_NAME::text,                          -- table_name
        TG_RELID,                                     -- relation OID for much quicker searches
        session_user::text,                           -- session_user_name
        clock_timestamp() at time zone 'America/Toronto', -- action_tstamp_clk
        txid_current(),                               -- transaction ID
        current_setting('application_name'),          -- client application
        current_query(),                              -- top-level query or queries (if multistatement) from client
        substring(TG_OP,1,1),                         -- action
        NULL, NULL,                                   -- row_data, changed_fields
        'f'                                           -- statement_only
    );

    IF NOT TG_ARGV[0]::boolean IS DISTINCT FROM 'f'::boolean THEN
        audit_row.client_query = NULL;
    END IF;

    IF TG_ARGV[1] IS NOT NULL THEN
        excluded_cols = TG_ARGV[1]::text[];
    END IF;
    
    IF (TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW') THEN
        audit_row.row_data = row_to_json(OLD)::JSONB - excluded_cols;
        
        --Computing differences
        SELECT
            jsonb_object_agg(tmp_new_row.key, tmp_new_row.value) AS new_data
            INTO audit_row.changed_fields
        FROM jsonb_each_text(row_to_json(NEW)::JSONB) AS tmp_new_row 
            JOIN jsonb_each_text(audit_row.row_data) AS tmp_old_row ON (tmp_new_row.key = tmp_old_row.key AND tmp_new_row.value IS DISTINCT FROM tmp_old_row.value);
        
        IF audit_row.changed_fields = '{}'::JSONB THEN
            -- All changed fields are ignored. Skip this update.
            RETURN NULL;
        END IF;
    ELSIF (TG_OP = 'DELETE' AND TG_LEVEL = 'ROW') THEN
        audit_row.row_data = row_to_json(OLD)::JSONB - excluded_cols;
    ELSIF (TG_OP = 'INSERT' AND TG_LEVEL = 'ROW') THEN
        audit_row.row_data = row_to_json(NEW)::JSONB - excluded_cols;
    ELSIF (TG_LEVEL = 'STATEMENT' AND TG_OP IN ('INSERT','UPDATE','DELETE','TRUNCATE')) THEN
        audit_row.statement_only = 't';
    ELSE
        RAISE EXCEPTION '[ecocounter.if_modified_func] - Trigger func added as trigger for unhandled case: %, %',TG_OP, TG_LEVEL;
        RETURN NULL;
    END IF;
    INSERT INTO ecocounter.logged_actions VALUES (audit_row.*);
    RETURN NULL;
END;
$BODY$;

ALTER FUNCTION ecocounter.if_modified_func()
    OWNER TO ecocounter_admins;

COMMENT ON FUNCTION ecocounter.if_modified_func()
IS '
Track changes to a table at the statement and/or row level.

Optional parameters to trigger in CREATE TRIGGER call:

param 0: boolean, whether to log the query text. Default ''t''.

param 1: text[], columns to ignore in updates. Default [].

         Updates to ignored cols are omitted from changed_fields.

         Updates with only ignored cols changed are not inserted
         into the audit log.

         Almost all the processing work is still done for updates
         that ignored. If you need to save the load, you need to use
         WHEN clause on the trigger instead.

         No warning or error is issued if ignored_cols contains columns
         that do not exist in the target table. This lets you specify
         a standard set of ignored columns.

There is no parameter to disable logging of values. Add this trigger as
a ''FOR EACH STATEMENT'' rather than ''FOR EACH ROW'' trigger if you do not
want to log row values.

Note that the user name logged is the login role for the session. The audit trigger
cannot obtain the active role because it is reset by the SECURITY DEFINER invocation
of the audit trigger its self.
';
