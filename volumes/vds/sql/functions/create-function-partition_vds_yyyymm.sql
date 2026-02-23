CREATE OR REPLACE FUNCTION vds.partition_vds_yyyymm(
    base_table text,
    year_ integer,
    datetime_col text)
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

BEGIN

    EXECUTE FORMAT(
        $SQL$
            CREATE TABLE IF NOT EXISTS vds.%I
            PARTITION OF vds.%I
            FOR VALUES FROM (%L) TO (%L)
            PARTITION BY RANGE (%I);
            ALTER TABLE IF EXISTS vds.%I OWNER TO vds_admins;
        $SQL$,
        year_table,
        base_table,
        startdate,
        enddate,
        datetime_col,
        year_table
    );

    FOR mm IN 01..12 LOOP
        start_mm:= to_date(year_::text||'-'||mm||'-01', 'YYYY-MM-DD');
		end_mm:= start_mm + INTERVAL '1 month';
        IF mm < 10 THEN
            tablename:= year_table||'0'||mm;
        ELSE
            tablename:= year_table||mm;
        END IF;
        EXECUTE FORMAT(
            $SQL$
                CREATE TABLE IF NOT EXISTS vds.%I 
                PARTITION OF vds.%I
                FOR VALUES FROM (%L) TO (%L);
                ALTER TABLE IF EXISTS vds.%I OWNER TO vds_admins;
            $SQL$,
            tablename,
            year_table,
            start_mm,
            end_mm,
            tablename
        );
    END LOOP;


END;
$BODY$;

COMMENT ON FUNCTION vds.partition_vds_yyyymm(text, integer, text) IS
'Create new partitions by year and month under the parent table `base_table`.
Can be used accross vds schema when partitioning by year, month. Use parameter
datetime_col to specify the partitioning timestamp column, ie. `dt`.
Example: SELECT vds.partition_vds_yyyymm(''raw_vdsdata_div8001'', 2023, ''dt'')
Example: SELECT vds.partition_vds_yyyymm(''raw_vdsvehicledata'', 2023, ''dt'')';

ALTER FUNCTION vds.partition_vds_yyyymm(text, integer, text) OWNER TO vds_admins;

GRANT EXECUTE ON FUNCTION vds.partition_vds_yyyymm(text, integer, text) TO vds_bot;