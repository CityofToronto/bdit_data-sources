/*
Parameters:
Name | Type | Description
base_table | text | The parent table under which the new partition(s) would be created, e.g., `vds.raw_vdsdata_div8001`
year_ | integer | The year whose data would be saved in the new partition, e.g., 2023
div_id | integer | Division_id, used to determine one or two yearly partitions, e.g., 8001

Return: Void
Purpose:
Create new partitions for the given division_id and year (if needed) under the parent table `base_table`
*/

CREATE OR REPLACE FUNCTION vds.partition_vdsdata(
    base_table text,
    year_ integer,
    div_id integer)
RETURNS void
LANGUAGE 'plpgsql'
SECURITY DEFINER
COST 100
VOLATILE PARALLEL UNSAFE

AS $BODY$

DECLARE
    part_table_1 TEXT;
    part_table_2 TEXT;
    start_1 DATE;
    start_2 DATE;
    end_date DATE;

BEGIN

IF div_id = 8001 THEN --create two partitions instead of 1

    part_table_1 = base_table || '_' || year_::text || '_01_06';
    part_table_2 = base_table || '_' || year_::text || '_07_12';
    start_1 = (year_::text || '-01-01')::date;
    start_2 = (year_::text || '-07-01')::date;
    end_date = ((year_+1)::text || '-01-01')::date;

    -- create the first half of the year partition
    EXECUTE FORMAT(
        $SQL$
            CREATE TABLE IF NOT EXISTS vds.%I
            PARTITION OF vds.%I
            FOR VALUES FROM (%L) TO (%L);
            ALTER TABLE IF EXISTS vds.%I OWNER TO vds_admins;
        $SQL$,
        part_table_1,
        base_table,
        start_1,
        start_2,
        part_table_1
    );
 
    -- create the second half of the year partition
    EXECUTE FORMAT(
        $SQL$
            CREATE TABLE IF NOT EXISTS vds.%I
            PARTITION OF vds.%I
            FOR VALUES FROM (%L) TO (%L);
            ALTER TABLE IF EXISTS vds.%I OWNER TO vds_admins;
        $SQL$,
        part_table_2,
        base_table,
        start_2,
        end_date,
        part_table_1
    );

ELSIF div_id = 2 THEN
    part_table_1 = base_table || '_' || year_::text;
    start_1 = (year_::text || '-01-01')::date;
    end_date = ((year_+1)::text || '-01-01')::date;

    EXECUTE FORMAT(
        $SQL$
            CREATE TABLE IF NOT EXISTS vds.%I
            PARTITION OF vds.%I
            FOR VALUES FROM (%L) TO (%L);
            ALTER TABLE IF EXISTS vds.%I OWNER TO vds_admins;
        $SQL$,
        part_table_1,
        base_table,
        start_1,
        end_date,
        part_table_1
    );
ELSE
  RAISE NOTICE 'Unknown division_id: %. No tables created.', div_id;
END IF;

END;
$BODY$;

COMMENT ON FUNCTION vds.partition_vdsdata(text, integer, integer) IS
'Create new partitions for the given division_id and year (if needed) under the parent table `base_table`.
Use for all of: vds.raw_vdsdata, vds.counts_15min, vds.counts_15min_bylane partitions. 
Example: SELECT vds.partition_vdsdata(''raw_vdsdata_div8001'', 2023, 8001)';

ALTER FUNCTION vds.partition_vdsdata(text, integer, integer) OWNER TO vds_admins;

GRANT EXECUTE ON FUNCTION vds.partition_vdsdata(text, integer, integer) TO vds_bot;