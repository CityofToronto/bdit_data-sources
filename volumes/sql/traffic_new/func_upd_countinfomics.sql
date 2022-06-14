-- FUNCTION: traffic.update_countinfomics()

-- DROP FUNCTION IF EXISTS traffic.update_countinfomics();

CREATE OR REPLACE FUNCTION traffic.update_countinfomics(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.countinfomics (
	select * from "TRAFFIC_NEW"."COUNTINFOMICS"
	except
	select * from traffic.countinfomics)
	on conflict (count_info_id) 
	DO UPDATE 
		SET arterycode = EXCLUDED.arterycode,
			count_type = EXCLUDED.count_type,
			count_date = EXCLUDED.count_date,
			day_no = EXCLUDED.day_no,
			comment_ = EXCLUDED.comment_,
			file_name = EXCLUDED.file_name,
			source1 = EXCLUDED.source1,
			source2 = EXCLUDED.source2,
			load_date = EXCLUDED.load_date,
			transfer_rec = EXCLUDED.transfer_rec,
			category_id = EXCLUDED.category_id;

with delrec as (
	select * from traffic.countinfomics 
	except 
	select * from "TRAFFIC_NEW"."COUNTINFOMICS")

Delete from traffic.countinfomics where count_info_id in (SELECT count_info_id from delrec);

$BODY$;

ALTER FUNCTION traffic.update_countinfomics()
    OWNER TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_countinfomics() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_countinfomics() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_countinfomics() FROM PUBLIC;
