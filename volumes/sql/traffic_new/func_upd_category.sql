-- FUNCTION: traffic.update_category()

-- DROP FUNCTION IF EXISTS traffic.update_category();

CREATE OR REPLACE FUNCTION traffic.update_category(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.category (
	select * from "TRAFFIC_NEW"."CATEGORY"
	except
	select * from traffic.category)
	on conflict (category_id) 
	DO UPDATE 
		SET category_name = EXCLUDED.category_name;

with delrec as (
	select * from traffic.category 
	except 
	select * from "TRAFFIC_NEW"."CATEGORY")

Delete from traffic.category where category_id in (SELECT category_id from delrec);

$BODY$;

ALTER FUNCTION traffic.update_category()
    OWNER TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_category() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_category() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_category() FROM PUBLIC;