CREATE OR REPLACE FUNCTION gwolofs.create_yyyy_partition(
    base_table text,
    year_ integer,
    _schema_name text,
    _owner text)
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
        CREATE TABLE IF NOT EXISTS %I.%I
        PARTITION OF %I.%I
        FOR VALUES FROM (%L) TO (%L);
        ALTER TABLE IF EXISTS %I.%I OWNER TO %I;
        $$,
        _schema_name,
        year_table,
        _schema_name,
        base_table,
        startdate,
        enddate,
        _schema_name,
        year_table,
        _owner
    );

END;
$BODY$;

COMMENT ON FUNCTION gwolofs.create_yyyy_partition(text, integer, text, text) IS
'Create a new year partition under the parent table `base_table`.
Can be used accross schemas when partitioning by year. 
Example: SELECT gwolofs.create_yyyy_partition(''raw_vdsvehicledata'', 2023, ''vds'', ''vds_admins'')';

--ALTER FUNCTION gwolofs.create_yyyy_partition(text, integer, text, text) OWNER TO vds_admins;
--GRANT EXECUTE ON FUNCTION gwolofs.create_yyyy_partition(text, integer, text, text) TO vds_bot;