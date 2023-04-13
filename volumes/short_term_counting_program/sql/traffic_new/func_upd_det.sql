-- FUNCTION: traffic.update_det()

-- DROP FUNCTION IF EXISTS traffic.update_det();

CREATE OR REPLACE FUNCTION traffic.update_det(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    TRUNCATE TABLE traffic.det;
	INSERT INTO traffic.det
	(Select * from "TRAFFIC_NEW"."DET");

$BODY$;

ALTER FUNCTION traffic.update_det()
    OWNER TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_det() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_det() TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_det() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_det() FROM PUBLIC;