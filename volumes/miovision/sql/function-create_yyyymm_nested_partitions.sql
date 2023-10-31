CREATE OR REPLACE FUNCTION gwolofs.create_yyyymm_nested_partitions(
    base_table text,
    year_ integer,
    datetime_col text,
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
    start_mm DATE;
    end_mm DATE;
	tablename TEXT;
    _mm TEXT;

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS %I.%I
        PARTITION OF %I.%I
        FOR VALUES FROM (%L) TO (%L)
        PARTITION BY RANGE (%I);
        ALTER TABLE IF EXISTS %I.%I OWNER TO %I;
        $$,
        _schema_name,
        year_table,
        _schema_name,
        base_table,
        startdate,
        enddate,
        datetime_col,
        _schema_name,
        year_table,
        _owner
    );

    FOR mm IN 1..12 LOOP
        _mm:=lpad(mm::text, 2, '0');
        start_mm:= to_date(year_::text||'-'||_mm||'-01', 'YYYY-MM-DD');
		end_mm:= start_mm + INTERVAL '1 month';
        tablename:= year_table||_mm;
        EXECUTE FORMAT($$
                CREATE TABLE IF NOT EXISTS %I.%I
                PARTITION OF %I.%I
                FOR VALUES FROM (%L) TO (%L);
                ALTER TABLE IF EXISTS %I.%I OWNER TO %I;
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
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION gwolofs.create_yyyymm_nested_partitions(text, integer, text, text, text) IS
'Create new partitions by year and month under the parent table `base_table`.
Can be used accross schemas when partitioning by year, month. Use parameter
datetime_col to specify the partitioning timestamp column, ie. `dt`.
Example: SELECT gwolofs.create_yyyymm_nested_partitions(''raw_vdsdata_div8001'', 2023, ''dt'')
Example: SELECT gwolofs.create_yyyymm_nested_partitions(''raw_vdsvehicledata'', 2023, ''dt'')';

--ALTER FUNCTION gwolofs.create_yyyymm_nested_partitions(text, integer, text, text, text) OWNER TO vds_admins;
--GRANT EXECUTE ON FUNCTION gwolofs.create_yyyymm_nested_partitions(text, integer, text, text, text) TO vds_bot;