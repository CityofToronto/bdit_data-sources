/*
Parameters:
Name | Type | Description
base_table | text | The parent table under which the new partition(s) would be created, e.g., `vds.raw_vdsdata_div8001`
year_ | integer | The year whose data would be saved in the new partition, e.g., 2023
div_id | integer | Division_id, used to determine one or two yearly partitions, e.g., 8001

Return: Void
Purpose:
Create new partitions for the given year (if needed) under the parent table `vds.raw_vdsvehicledata`
*/

CREATE OR REPLACE FUNCTION vds.partition_vdsvehicledata(year_ integer)
RETURNS void
LANGUAGE 'plpgsql'
SECURITY DEFINER
COST 100
VOLATILE PARALLEL UNSAFE

AS $BODY$

DECLARE
	startdate DATE := (year_::text || '-01-01')::date;
	enddate DATE := ((year_+1)::text || '-01-01')::date;
    start_mm DATE;
    end_mm DATE;
	basetablename TEXT := 'raw_vdsvehicledata_'||year_::text;
	tablename TEXT;

BEGIN

    EXECUTE FORMAT(
        $SQL$
            CREATE TABLE IF NOT EXISTS vds.%I
            PARTITION OF vds.raw_vdsvehicledata
            FOR VALUES FROM (%L) TO (%L)
            PARTITION BY RANGE (dt);
            ALTER TABLE IF EXISTS vds.%I OWNER TO vds_admins;
        $SQL$,
        basetablename,
        startdate,
        enddate,
        basetablename
    );

    FOR mm IN 01..12 LOOP
        start_mm:= to_date(year_::text||'-'||mm||'-01', 'YYYY-MM-DD');
		end_mm:= start_mm + INTERVAL '1 month';
        IF mm < 10 THEN
            tablename:= basetablename||'0'||mm;
        ELSE
            tablename:= basetablename||mm;
        END IF;
        EXECUTE FORMAT(
            $SQL$
                CREATE TABLE IF NOT EXISTS vds.%I 
                PARTITION OF vds.%I
                FOR VALUES FROM (%L) TO (%L);
                ALTER TABLE IF EXISTS vds.%I OWNER TO vds_admins;
            $SQL$,
            tablename,
            basetablename,
            start_mm,
            end_mm,
            tablename
        );
    END LOOP;

END;
$BODY$;

COMMENT ON FUNCTION vds.partition_vdsvehicledata(integer) IS
'Create new partitions for the given year and subpartitions by month under the parent table `vds.raw_vdsvehicledata`.
Example: SELECT vds.partition_vdsvehicledata(2023)';

ALTER FUNCTION vds.partition_vdsvehicledata(integer) OWNER TO vds_admins;

GRANT EXECUTE ON FUNCTION vds.partition_vdsvehicledata(integer) TO vds_bot;