-- FUNCTION: collisions_replicator.refresh_ev_mv()

-- DROP FUNCTION IF EXISTS collisions_replicator.refresh_ev_mv();

CREATE OR REPLACE FUNCTION collisions_replicator.refresh_ev_mv(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY collisions_replicator.events WITH DATA;
$BODY$;

ALTER FUNCTION collisions_replicator.refresh_ev_mv()
    OWNER TO scannon;

GRANT EXECUTE ON FUNCTION collisions_replicator.refresh_ev_mv() TO collisions_bot;

GRANT EXECUTE ON FUNCTION collisions_replicator.refresh_ev_mv() TO scannon;

REVOKE ALL ON FUNCTION collisions_replicator.refresh_ev_mv() FROM PUBLIC;

COMMENT ON FUNCTION collisions_replicator.refresh_ev_mv()
    IS '
Refreshes collisions_replicator.events mat view with data. Called by a DAG at 3am daily.
';