CREATE OR REPLACE FUNCTION ptc.create_annual_partition(
    base_table text,
    year_ integer
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
COST 100
VOLATILE PARALLEL UNSAFE

AS $BODY$

DECLARE
    year_table TEXT := base_table||'_'||year_::text;
    startdate DATE := (year_::text || '-01-01')::date;
    tablename TEXT;

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS ptc.%1$I
        PARTITION OF ptc.%2$I
        FOR VALUES FROM (%3$L) TO (%3$L::date + interval '1 year');
        ALTER TABLE IF EXISTS ptc.%1$I OWNER TO ptc_admins;
        GRANT ALL ON TABLE ptc.%1$I TO ptc_admins;
        REVOKE ALL ON TABLE ptc.%1$I FROM bdit_humans;
        GRANT SELECT ON TABLE ptc.%1$I TO bdit_humans;
        $$,
        year_table,
        base_table,
        startdate
    );

END;
$BODY$;

COMMENT ON FUNCTION ptc.create_annual_partition(text, integer) IS
'''Create a new year partition under the parent table `base_table`.
Only to be used for ptc schema.
Example: `SELECT ptc.create_annual_partition(''open_data_trips'', 2023)`''';

ALTER FUNCTION ptc.create_annual_partition(text, integer)
OWNER TO ptc_admins;

GRANT EXECUTE ON FUNCTION ptc.create_annual_partition(text, integer)
TO ptc_bot;
