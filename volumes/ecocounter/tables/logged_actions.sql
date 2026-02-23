-- Table: ecocounter.logged_actions

-- DROP TABLE IF EXISTS ecocounter.logged_actions;

CREATE TABLE IF NOT EXISTS ecocounter.logged_actions
(
    event_id bigint NOT NULL DEFAULT nextval('ecocounter.logged_actions_event_id_seq'::regclass),
    schema_name text COLLATE pg_catalog."default" NOT NULL,
    table_name text COLLATE pg_catalog."default" NOT NULL,
    relid oid NOT NULL,
    session_user_name text COLLATE pg_catalog."default",
    action_tstamp_clk timestamp with time zone NOT NULL,
    transaction_id bigint,
    application_name text COLLATE pg_catalog."default",
    client_query text COLLATE pg_catalog."default",
    action text COLLATE pg_catalog."default" NOT NULL,
    row_data jsonb,
    changed_fields jsonb,
    statement_only boolean NOT NULL,
    CONSTRAINT logged_actions_pkey PRIMARY KEY (event_id),
    CONSTRAINT logged_actions_action_check CHECK (action = ANY (ARRAY['I'::text, 'D'::text, 'U'::text, 'T'::text]))
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecocounter.logged_actions OWNER TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.logged_actions FROM bdit_humans;

GRANT INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE ecocounter.logged_actions TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE ecocounter.logged_actions TO ecocounter_admins;

GRANT ALL ON TABLE ecocounter.logged_actions TO rds_superuser WITH GRANT OPTION;

COMMENT ON TABLE ecocounter.logged_actions
IS 'History of auditable actions on audited tables, from ecocounter.if_modified_func()';

COMMENT ON COLUMN ecocounter.logged_actions.event_id
IS 'Unique identifier for each auditable event';

COMMENT ON COLUMN ecocounter.logged_actions.schema_name
IS 'Database schema audited table for this event is in';

COMMENT ON COLUMN ecocounter.logged_actions.table_name
IS 'Non-schema-qualified table name of table event occured in';

COMMENT ON COLUMN ecocounter.logged_actions.relid
IS 'Table OID. Changes with drop/create. Get with ''tablename''::regclass';

COMMENT ON COLUMN ecocounter.logged_actions.session_user_name
IS 'Login / session user whose statement caused the audited event';

COMMENT ON COLUMN ecocounter.logged_actions.action_tstamp_clk
IS 'Wall clock time at which audited event''s trigger call occurred';

COMMENT ON COLUMN ecocounter.logged_actions.transaction_id
IS 'Identifier of transaction that made the change. May wrap, but unique paired.';

COMMENT ON COLUMN ecocounter.logged_actions.application_name
IS 'Application name set when this audit event occurred. Can be changed in-session by client.';

COMMENT ON COLUMN ecocounter.logged_actions.client_query
IS 'Top-level query that caused this auditable event. May be more than one statement.';

COMMENT ON COLUMN ecocounter.logged_actions.action
IS 'Action type; I = insert, D = delete, U = update, T = truncate';

COMMENT ON COLUMN ecocounter.logged_actions.row_data
IS 'Record value. Null for statement-level trigger. For INSERT this is the new tuple. For DELETE and UPDATE it is the old tuple.';

COMMENT ON COLUMN ecocounter.logged_actions.changed_fields
IS 'New values of fields changed by UPDATE. Null except for row-level UPDATE events.';

COMMENT ON COLUMN ecocounter.logged_actions.statement_only
IS '''t'' if audit event is from an FOR EACH STATEMENT trigger, ''f'' for FOR EACH ROW';
-- Index: logged_actions_action_idx

-- DROP INDEX IF EXISTS ecocounter.logged_actions_action_idx;

CREATE INDEX IF NOT EXISTS logged_actions_action_idx
ON ecocounter.logged_actions USING btree
(action COLLATE pg_catalog."default" ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True)
TABLESPACE pg_default;
-- Index: logged_actions_action_tstamp_clk_idx

-- DROP INDEX IF EXISTS ecocounter.logged_actions_action_tstamp_clk_idx;

CREATE INDEX IF NOT EXISTS logged_actions_action_tstamp_clk_idx
ON ecocounter.logged_actions USING brin
(action_tstamp_clk)
WITH (pages_per_range=128, autosummarize=False)
TABLESPACE pg_default;
-- Index: logged_actions_table_name_idx

-- DROP INDEX IF EXISTS ecocounter.logged_actions_table_name_idx;

CREATE INDEX IF NOT EXISTS logged_actions_table_name_idx
ON ecocounter.logged_actions USING btree
(table_name COLLATE pg_catalog."default" ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True)
TABLESPACE pg_default;


ALTER SEQUENCE ecocounter.logged_actions_event_id_seq
OWNED BY ecocounter.logged_actions.event_id;

ALTER SEQUENCE ecocounter.logged_actions_event_id_seq
OWNER TO ecocounter_admins;
