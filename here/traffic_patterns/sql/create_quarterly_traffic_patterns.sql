CREATE OR REPLACE FUNCTION here.create_quarterly_traffic_patterns(
    _routing_table_version text,
    _start_date date,
    _end_date date)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE
    output_table TEXT;
    routing_table TEXT;

BEGIN
    output_table := 'traffic_pattern_'||_routing_table_version||'_'||EXTRACT(YEAR FROM _start_date)::text||'q'||EXTRACT(QUARTER FROM _start_date)::text;
    routing_table := 'routing_streets_'||_routing_table_version;

    EXECUTE format($$CREATE TABLE here.%I AS
                    WITH hourly_time_cost AS (
                        SELECT
                            routing_streets.link_dir,
                            dt,
                            date_trunc('hour', ta.tod)::time AS hr,
                            harmean(ta.pct_50) AS daily_cost
                        FROM here.%I AS routing_streets
                        LEFT JOIN here.ta USING (link_dir)
                        LEFT JOIN ref.holiday USING (dt)
                        WHERE
                            dt >= %L AND dt < %L
                            AND EXTRACT(ISODOW FROM dt) IN (2, 3, 4) -- only include tues-thurs traffic
                            AND holiday.dt IS NULL -- excluding holidays
                        GROUP BY routing_streets.link_dir, dt, hr
                    )

                    SELECT
                        link_dir,
                        hr,
                        harmean(daily_cost)::int AS spd
                    FROM hourly_time_cost
                    GROUP BY link_dir, hr; $$,  
                                       output_table, routing_table,  _start_date, _end_date
                                       );
END;
$BODY$;

ALTER FUNCTION here.create_quarterly_traffic_patterns(text, date, date) OWNER TO here_admins;
COMMENT ON FUNCTION here.create_quarterly_traffic_patterns(text, date, date) IS '''
    Function to create quarterly traffic patterns based on the input routing_street version, start and end date. 
    Quarter number is generated using the start_date, does not take in account in the end date. 
    Example Usage:
    To create a table for routing version 22_2_b, for the dates between 2021 Jan 4 to 2021 Apr 5th aka 2021 quarter 1
    SELECT here.create_quarterly_traffic_patterns(''22_2_b'', ''2021-01-04'', ''2021-04-05''); 
    '''