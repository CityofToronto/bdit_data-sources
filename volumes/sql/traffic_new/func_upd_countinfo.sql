-- FUNCTION: traffic.update_countinfo()

-- DROP FUNCTION IF EXISTS traffic.update_countinfo();

CREATE OR REPLACE FUNCTION traffic.update_countinfo(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.countinfo (
	select * from "TRAFFIC_NEW"."COUNTINFO"
	except
	select * from traffic.countinfo)
	on conflict (count_info_id) 
	DO UPDATE 
		SET arterycode = EXCLUDED.arterycode,
			count_date = EXCLUDED.count_date,
			day_no = EXCLUDED.day_no,
			comment_ = EXCLUDED.comment_,
			file_name = EXCLUDED.file_name,
			source1 = EXCLUDED.source1,
			source2 = EXCLUDED.source2,
			load_date = EXCLUDED.load_date,
			speed_info_id = EXCLUDED.speed_info_id,
			category_id = EXCLUDED.category_id;

with delrec as (
	select * from traffic.countinfo 
	except 
	select * from "TRAFFIC_NEW"."COUNTINFO")

Delete from traffic.countinfo where count_info_id in (SELECT count_info_id from delrec);

$BODY$;

ALTER FUNCTION traffic.update_countinfo()
    OWNER TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_countinfo() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_countinfo() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_countinfo() FROM PUBLIC;