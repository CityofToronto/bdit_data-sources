CREATE OR REPLACE FUNCTION wys.create_mm_nested_raw_data_partitions(
    year_ integer,
    mm_ integer)
RETURNS void
LANGUAGE 'plpgsql'
SECURITY DEFINER
COST 100
VOLATILE PARALLEL UNSAFE

AS $BODY$

DECLARE
	year_table TEXT := 'raw_data_'||year_::text;
    start_mm DATE;
    end_mm DATE;
	month_table TEXT;
    mm_pad TEXT;

BEGIN
    mm_pad:=lpad(mm_::text, 2, '0');
    start_mm:= to_date(year_::text||'-'||mm_pad||'-01', 'YYYY-MM-DD');
    end_mm:= start_mm + INTERVAL '1 month';
    month_table:= year_table||mm_pad;
    EXECUTE FORMAT($$
            CREATE TABLE IF NOT EXISTS wys.%I
            PARTITION OF wys.%I
            FOR VALUES FROM (%L) TO (%L);
            ALTER TABLE IF EXISTS wys.%I OWNER TO wys_admins;
            GRANT SELECT, REFERENCES ON TABLE wys.%I TO bdit_humans WITH GRANT OPTION;
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE wys.%I TO wys_bot;
        $$,
        month_table,
        year_table,
        start_mm,
        end_mm,
        month_table,
        month_table,
        month_table
    );
END;
$BODY$;

COMMENT ON FUNCTION wys.create_mm_nested_raw_data_partitions(integer, integer) IS
'Create a new month partition under the parent wys `raw_data` year table.
Example: SELECT wys.create_mm_nested_raw_data_partitions(2023, 1)';

ALTER FUNCTION wys.create_mm_nested_raw_data_partitions(integer, integer) OWNER TO wys_admins;
GRANT EXECUTE ON FUNCTION wys.create_mm_nested_raw_data_partitions(integer, integer) TO wys_bot;