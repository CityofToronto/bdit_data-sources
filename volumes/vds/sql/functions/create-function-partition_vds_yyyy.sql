CREATE OR REPLACE FUNCTION vds.partition_vds_yyyy(
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

    EXECUTE FORMAT(
        $SQL$
            CREATE TABLE IF NOT EXISTS vds.%I
            PARTITION OF vds.%I
            FOR VALUES FROM (%L) TO (%L);
            ALTER TABLE IF EXISTS vds.%I OWNER TO vds_admins;
        $SQL$,
        year_table,
        base_table,
        startdate,
        enddate,
        year_table
    );

END;
$BODY$;

COMMENT ON FUNCTION vds.partition_vds_yyyy(text, integer) IS
'Create new partition by year under the parent table `base_table`.
Can be used accross vds schema when partitioning by year, month and date column is `dt`. 
Example: SELECT vds.partition_vds_yyyy(''raw_vdsdata_div8001'', 2023)
Example: SELECT vds.partition_vds_yyyy(''raw_vdsvehicledata'', 2023)';

ALTER FUNCTION vds.partition_vds_yyyy(text, integer) OWNER TO vds_admins;

GRANT EXECUTE ON FUNCTION vds.partition_vds_yyyy(text, integer) TO vds_bot;