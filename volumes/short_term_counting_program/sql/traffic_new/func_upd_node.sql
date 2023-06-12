-- FUNCTION: traffic.update_node()

-- DROP FUNCTION IF EXISTS traffic.update_node();

CREATE OR REPLACE FUNCTION traffic.update_node(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.node (
	select * from "TRAFFIC_NEW"."NODE"
	except
	select * from traffic.node)
	on conflict (node_id) 
	DO UPDATE 
		SET link_id = EXCLUDED.link_id,
			x_coord = EXCLUDED.x_coord,
			y_coord = EXCLUDED.y_coord,
			ts = EXCLUDED.ts;

with delrec as (
	select * from traffic.node 
	except 
	select * from "TRAFFIC_NEW"."NODE")

Delete from traffic.node where node_id in (SELECT node_id from delrec);

$BODY$;

ALTER FUNCTION traffic.update_node()
    OWNER TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_node() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_node() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_node() FROM PUBLIC;