-- FUNCTION: traffic.update_arterydata()

-- DROP FUNCTION IF EXISTS traffic.update_arterydata();

CREATE OR REPLACE FUNCTION traffic.update_arterydata(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.arterydata (
	select * from "TRAFFIC_NEW"."ARTERYDATA"
	except
	select * from traffic.arterydata)
	on conflict (arterycode) 
	DO UPDATE 
		SET geomcode = EXCLUDED.geomcode,
			street1 = EXCLUDED.street1,
			street1type = EXCLUDED.street1type,
			street1dir = EXCLUDED.street1dir,
			street2 = EXCLUDED.street2,
			street2type = EXCLUDED.street2type,
			street2dir = EXCLUDED.street2dir,
			street3 = EXCLUDED.street3,
			street3type = EXCLUDED.street3type,
			street3dir = EXCLUDED.street3dir,
			stat_code = EXCLUDED.stat_code,
			count_type = EXCLUDED.count_type,
			location = EXCLUDED.location,
			apprdir = EXCLUDED.apprdir,
			sideofint = EXCLUDED.sideofint,
			linkid = EXCLUDED.linkid,
			seq_order = EXCLUDED.seq_order,
			geo_id = EXCLUDED.geo_id;

with delrec as (
	select * from traffic.arterydata 
	except 
	select * from "TRAFFIC_NEW"."ARTERYDATA")

Delete from traffic.arterydata where arterycode in (SELECT arterycode from delrec);

$BODY$;

ALTER FUNCTION traffic.update_arterydata()
    OWNER TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_arterydata() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_arterydata() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_arterydata() FROM PUBLIC;