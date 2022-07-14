-- FUNCTION: traffic.update_cnt_spd()

-- DROP FUNCTION IF EXISTS traffic.update_cnt_spd();

CREATE OR REPLACE FUNCTION traffic.update_cnt_spd(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.cnt_spd (
	select * from "TRAFFIC_NEW"."CNT_SPD"
	except
	select * from traffic.cnt_spd)
	on conflict (id) 
	DO UPDATE 
		SET count_info_id = EXCLUDED.count_info_id,
			speed = EXCLUDED.speed,
			timecount = EXCLUDED.timecount;

with delrec as (
	select * from traffic.cnt_spd 
	except 
	select * from "TRAFFIC_NEW"."CNT_SPD")

Delete from traffic.cnt_spd where id in (SELECT id from delrec);

$BODY$;

ALTER FUNCTION traffic.update_cnt_spd()
    OWNER TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_cnt_spd() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_cnt_spd() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_cnt_spd() FROM PUBLIC;