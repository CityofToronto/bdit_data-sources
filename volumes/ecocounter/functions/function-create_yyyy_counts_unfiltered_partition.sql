CREATE OR REPLACE FUNCTION ecocounter.create_yyyy_counts_unfiltered_partition(
    base_table text,
    year_ integer
)
RETURNS void
LANGUAGE 'plpgsql'
SECURITY DEFINER
COST 100
VOLATILE PARALLEL UNSAFE

AS $BODY$

DECLARE
	year_table TEXT := base_table||'_'||year_::text;
	startdate DATE := (year_::text || '-01-01')::date;
	enddate DATE := ((year_+1)::text || '-01-01')::date;
	tablename TEXT;

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS ecocounter.%I
        PARTITION OF ecocounter.%I
        FOR VALUES FROM (%L) TO (%L);
        ALTER TABLE IF EXISTS ecocounter.%I OWNER TO ecocounter_admins;
        REVOKE ALL ON TABLE ecocounter.%I FROM bdit_humans;
        GRANT SELECT, INSERT, DELETE ON TABLE ecocounter.%I TO ecocounter_bot;
        $$,
        year_table,
        base_table,
        startdate,
        enddate,
        year_table,
        year_table,
        year_table
    );

END;
$BODY$;

COMMENT ON FUNCTION ecocounter.create_yyyy_counts_unfiltered_partition(text, integer) IS
'''Create a new year partition under the parent table `base_table`. Only to be used for
ecocounter `counts_unfiltered`.
Example: `SELECT ecocounter.create_yyyy_counts_unfiltered_partition(''counts_unfiltered'', 2023)`''';

ALTER FUNCTION ecocounter.create_yyyy_counts_unfiltered_partition(text, integer)
OWNER TO ecocounter_admins;

GRANT EXECUTE ON FUNCTION ecocounter.create_yyyy_counts_unfiltered_partition(text, integer)
TO ecocounter_bot;