CREATE OR REPLACE FUNCTION miovision_api.create_mm_nested_volumes_partitions(
    base_table text,
    year_ integer,
    mm_ integer)
RETURNS void
LANGUAGE 'plpgsql'
SECURITY DEFINER
COST 100
VOLATILE PARALLEL UNSAFE

AS $BODY$

DECLARE
	year_table TEXT := base_table||'_'||year_::text;
    start_mm DATE;
    end_mm DATE;
	tablename TEXT;

BEGIN
    _mm:=lpad(mm_::text, 2, '0');
    start_mm:= to_date(year_::text||'-'||_mm||'-01', 'YYYY-MM-DD');
    end_mm:= start_mm + INTERVAL '1 month';
    tablename:= year_table||_mm;
    EXECUTE FORMAT($$
            CREATE TABLE IF NOT EXISTS miovision_api.%I
            PARTITION OF miovision_api.%I
            FOR VALUES FROM (%L) TO (%L);
            ALTER TABLE IF EXISTS miovision_api.%I OWNER TO miovision_admins;
        $$,
        _schema_name,
        tablename,
        _schema_name,
        year_table,
        start_mm,
        end_mm,
        _schema_name,
        tablename,
        _owner
    );
END;
$BODY$;

COMMENT ON FUNCTION miovision_api.create_mm_nested_volumes_partitions(text, integer, integer) IS
'Create new partitions by year and month under the parent table `base_table`.
Can be used accross schemas when partitioning by year, month. Use parameter
datetime_col to specify the partitioning timestamp column, ie. `dt`.
Example: SELECT miovision_api.create_mm_nested_volumes_partitions(''raw_vdsdata_div8001'', 2023, ''dt'')
Example: SELECT miovision_api.create_mm_nested_volumes_partitions(''raw_vdsvehicledata'', 2023, ''dt'')';

--ALTER FUNCTION miovision_api.create_mm_nested_volumes_partitions(text, integer, integer) OWNER TO vds_admins;
--GRANT EXECUTE ON FUNCTION miovision_api.create_mm_nested_volumes_partitions(text, integer, integer) TO vds_bot;