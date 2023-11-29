CREATE OR REPLACE FUNCTION wys.create_yyyy_raw_data_partition(
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

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS wys.%I
        PARTITION OF wys.%I
        FOR VALUES FROM (%L) TO (%L)
        PARTITION BY RANGE (%I);
        ALTER TABLE IF EXISTS wys.%I OWNER TO wys_admins;
        GRANT SELECT, INSERT, UPDATE ON TABLE wys.%I TO wys_bot;
        GRANT SELECT, REFERENCES ON TABLE wys.%I TO bdit_humans WITH GRANT OPTION;
        $$,
        year_table,
        base_table,
        startdate,
        enddate,
        datetime_col,
        year_table,
        year_table,
        year_table
    );

END;
$BODY$;

COMMENT ON FUNCTION wys.create_yyyy_raw_data_partition(text, integer, text) IS
'Create a new year partition under the parent table `base_table`.
Only to be used for wys `raw_data` table. 
Use parameter `datetime_col` to specify the partitioning timestamp column, ie. `datetime_bin`.
Example: SELECT wys.create_yyyy_raw_data_partition(''raw_data'', 2023, ''datetime_bin'')';

ALTER FUNCTION wys.create_yyyy_raw_data_partition(text, integer, text) OWNER TO wys_admins;
GRANT EXECUTE ON FUNCTION wys.create_yyyy_raw_data_partition(text, integer, text) TO wys_bot;