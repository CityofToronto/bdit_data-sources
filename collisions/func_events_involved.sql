-- FUNCTION: collisions_replicator.refresh_mat_views()

-- DROP FUNCTION IF EXISTS collisions_replicator.refresh_mat_views();

CREATE OR REPLACE FUNCTION collisions_replicator.refresh_mat_views(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY collisions_replicator.events WITH DATA;
	REFRESH MATERIALIZED VIEW CONCURRENTLY collisions_replicator.involved WITH DATA;
$BODY$;

ALTER FUNCTION collisions_replicator.refresh_mat_views()
    OWNER TO scannon;

GRANT EXECUTE ON FUNCTION collisions_replicator.refresh_mat_views() TO collisions_bot;

GRANT EXECUTE ON FUNCTION collisions_replicator.refresh_mat_views() TO scannon;

REVOKE ALL ON FUNCTION collisions_replicator.refresh_mat_views() FROM PUBLIC;