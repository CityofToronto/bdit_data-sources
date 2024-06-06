CREATE OR REPLACE FUNCTION here.create_speed_limit(_routing_table_version text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE
	output_table TEXT;
	routing_table TEXT;
    streets_att_table TEXT:

BEGIN
	output_table := 'routing_streets_'||_routing_table_version||'_speed_limit';
	routing_table := 'routing_streets_'||_routing_table_version;
    street_att_table := 'streets_att_'||SUBSTRING(_routing_table_version FROM 1 FOR 4)::text; -- for when routing_streets has a suffix, e.g. _b, _tc

	EXECUTE format($$CREATE TABLE here.%I AS
                    SELECT
                        routing_streets.link_dir,
                        COALESCE(
                            CASE
                                WHEN RIGHT(routing_streets.link_dir, 1) = 'T' AND att.to_spd_lim > 0 THEN att.to_spd_lim
                                WHEN RIGHT(routing_streets.link_dir, 1) = 'F' AND att.fr_spd_lim > 0 THEN att.fr_spd_lim
                                ELSE NULL
                            END, 50
                        ) AS spd_lim
                    FROM here.%I AS routing_streets
                    LEFT JOIN here_gis.%I AS att ON LEFT(routing_streets.link_dir, -1)::int = att.link_id; $$,
                    output_table, routing_table, streets_att_table);

END;
$BODY$;


ALTER FUNCTION here.create_speed_limit(text) OWNER TO here_admins;
COMMENT ON FUNCTION here.create_speed_limit(text) IS '''
    Function to create speed limit table based on the input routing_street version. 
    If no speed limit information is available, a speed limit of 50 kmh is applied.
    
    Example Usage:
    To create a table for routing version 22_2_b.
    SELECT here.create_speed_limit(''22_2_b''); 
    '''
