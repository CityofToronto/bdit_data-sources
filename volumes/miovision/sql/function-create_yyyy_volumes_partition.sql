CREATE OR REPLACE FUNCTION miovision_api.create_yyyy_volumes_partition(
    base_table text,
    year_ integer)
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

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS miovision_api.%I
        PARTITION OF miovision_api.%I
        FOR VALUES FROM (%L) TO (%L)
        PARTITION BY RANGE (%I);
        ALTER TABLE IF EXISTS miovision_api.%I OWNER TO miovision_admins;
        $$,
        year_table,
        base_table,
        startdate,
        enddate,
        datetime_col,
        year_table
    );

END;
$BODY$;

COMMENT ON FUNCTION miovision_api.create_yyyy_volumes_partition(text, integer) IS
'Create a new year partition under the parent table `base_table`.
Can be used accross schemas when partitioning by year. 
Example: SELECT miovision_api.create_yyyy_volumes_partition(''raw_vdsvehicledata'', 2023, ''vds'', ''vds_admins'')';

ALTER FUNCTION miovision_api.create_yyyy_volumes_partition(text, integer) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.create_yyyy_volumes_partition(text, integer) TO miovision_api_bot;