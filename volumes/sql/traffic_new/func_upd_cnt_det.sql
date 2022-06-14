-- FUNCTION: traffic.update_cnt_det()

-- DROP FUNCTION IF EXISTS traffic.update_cnt_det();

CREATE OR REPLACE FUNCTION traffic.update_cnt_det(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.cnt_det (
	select * from "TRAFFIC_NEW"."CNT_DET"
	except
	select * from traffic.cnt_det)
	on conflict (id) 
	DO UPDATE 
		SET count_info_id = EXCLUDED.count_info_id,
			count = EXCLUDED.count,
			timecount = EXCLUDED.timecount,
			speed_class = EXCLUDED.speed_class;

with delrec as (
	select * from traffic.cnt_det 
	except 
	select * from "TRAFFIC_NEW"."CNT_DET")

Delete from traffic.cnt_det where id in (SELECT id from delrec);

$BODY$;

ALTER FUNCTION traffic.update_cnt_det()
    OWNER TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_cnt_det() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_cnt_det() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_cnt_det() FROM PUBLIC;