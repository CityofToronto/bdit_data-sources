-- FUNCTION: collisions_replicator.refresh_mat_view_collisions_no()

-- DROP FUNCTION IF EXISTS collisions_replicator.refresh_mat_view_collisions_no();

CREATE OR REPLACE FUNCTION collisions_replicator.refresh_mat_view_collisions_no(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY collisions_replicator.collisions_no WITH DATA ;
$BODY$;

ALTER FUNCTION collisions_replicator.refresh_mat_view_collisions_no()
    OWNER TO scannon;

GRANT EXECUTE ON FUNCTION collisions_replicator.refresh_mat_view_collisions_no() TO replicator_bot;

GRANT EXECUTE ON FUNCTION collisions_replicator.refresh_mat_view_collisions_no() TO scannon;

REVOKE ALL ON FUNCTION collisions_replicator.refresh_mat_view_collisions_no() FROM PUBLIC;