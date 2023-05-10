-- FUNCTION: traffic.update_arc_link()

-- DROP FUNCTION IF EXISTS traffic.update_arc_link();

CREATE OR REPLACE FUNCTION traffic.update_arc_link(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.arc_link (
	select * from "TRAFFIC_NEW"."ARC_LINK"
	except
	select * from traffic.arc_link)
	on conflict (link_id) 
	DO UPDATE 
		SET arc_id = EXCLUDED.arc_id,
			frequency = EXCLUDED.frequency;

with delrec as (
	select * from traffic.arc_link 
	except 
	select * from "TRAFFIC_NEW"."ARC_LINK")

Delete from traffic.arc_link where link_id in (SELECT link_id from delrec);

$BODY$;

ALTER FUNCTION traffic.update_arc_link()
    OWNER TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_arc_link() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_arc_link() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_arc_link() FROM PUBLIC;