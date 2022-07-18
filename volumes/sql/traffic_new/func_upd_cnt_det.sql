-- FUNCTION: traffic.update_cnt_det()

-- DROP FUNCTION IF EXISTS traffic.update_cnt_det();

CREATE OR REPLACE FUNCTION traffic.update_cnt_det(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

TRUNCATE TABLE traffic.cnt_det;
INSERT INTO traffic.cnt_det
(select * from "TRAFFIC_NEW"."CNT_DET")

$BODY$;

ALTER FUNCTION traffic.update_cnt_det()
    OWNER TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_cnt_det() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_cnt_det() TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_cnt_det() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_cnt_det() FROM PUBLIC;