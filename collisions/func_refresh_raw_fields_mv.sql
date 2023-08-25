-- FUNCTION: collisions_replicator.refresh_raw_fields_mv()

-- DROP FUNCTION IF EXISTS collisions_replicator.refresh_raw_fields_mv();

CREATE OR REPLACE FUNCTION collisions_replicator.refresh_raw_fields_mv()
RETURNS void
LANGUAGE 'sql'
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY collisions_replicator.events_fields_raw WITH DATA;
	REFRESH MATERIALIZED VIEW CONCURRENTLY collisions_replicator.involved_fields_raw WITH DATA;
$BODY$;

ALTER FUNCTION collisions_replicator.refresh_raw_fields_mv()
OWNER TO collision_admins;

GRANT EXECUTE ON FUNCTION collisions_replicator.refresh_raw_fields_mv() TO collisions_bot;

GRANT EXECUTE ON FUNCTION collisions_replicator.refresh_raw_fields_mv() TO scannon;

REVOKE ALL ON FUNCTION collisions_replicator.refresh_raw_fields_mv() FROM public;

COMMENT ON FUNCTION collisions_replicator.refresh_raw_fields_mv()
IS '
Refreshes collisions_replicator.events_fields_raw and collisions_replicator.involved_fields_raw mat views with data. 
Called by the collisions_replicator_transfer.py DAG at 3am daily.
';
