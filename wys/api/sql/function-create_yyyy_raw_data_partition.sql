CREATE OR REPLACE FUNCTION wys.create_yyyy_raw_data_partition(
    year_ integer
)
RETURNS void
LANGUAGE 'plpgsql'
SECURITY DEFINER
COST 100
VOLATILE PARALLEL UNSAFE

AS $BODY$

DECLARE
	year_table TEXT := 'raw_data_'||year_::text;
	startdate DATE := (year_::text || '-01-01')::date;
	enddate DATE := ((year_+1)::text || '-01-01')::date;

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS wys.%I
        PARTITION OF wys.raw_data
        FOR VALUES FROM (%L) TO (%L)
        PARTITION BY RANGE (datetime_bin);
        ALTER TABLE IF EXISTS wys.%I OWNER TO wys_admins;
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE wys.%I TO wys_bot;
        GRANT SELECT, REFERENCES ON TABLE wys.%I TO bdit_humans WITH GRANT OPTION;
        $$,
        year_table,
        startdate,
        enddate,
        year_table,
        year_table,
        year_table
    );

END;
$BODY$;

COMMENT ON FUNCTION wys.create_yyyy_raw_data_partition(integer) IS
'Create a new year partition under the parent table `wys.raw_data`, subpartitioned by column datetime_bin. 
Example: SELECT wys.create_yyyy_raw_data_partition(2023)';

ALTER FUNCTION wys.create_yyyy_raw_data_partition(integer) OWNER TO wys_admins;
GRANT EXECUTE ON FUNCTION wys.create_yyyy_raw_data_partition(integer) TO wys_bot;