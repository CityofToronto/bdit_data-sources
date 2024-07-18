CREATE OR REPLACE FUNCTION miovision_api.create_yyyy_volumes_partition(
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
        CREATE TABLE IF NOT EXISTS miovision_api.%I
        PARTITION OF miovision_api.%I
        FOR VALUES FROM (%L) TO (%L)
        PARTITION BY RANGE (%I);
        ALTER TABLE IF EXISTS miovision_api.%I OWNER TO miovision_admins;
        GRANT SELECT, INSERT, UPDATE ON TABLE miovision_api.%I TO miovision_api_bot;
        GRANT SELECT, REFERENCES ON TABLE miovision_api.%I TO bdit_humans WITH GRANT OPTION;
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

COMMENT ON FUNCTION miovision_api.create_yyyy_volumes_partition(text, integer, text)
IS '''Create a new year partition under the parent table `base_table`. Only to be used for
miovision_api `volumes` table. Use parameter `datetime_col` to specify the partitioning 
timestamp column, ie. `datetime_bin`.
Example: `SELECT miovision_api.create_yyyy_volumes_partition(''volumes'', 2023, ''datetime_bin'')`''';

ALTER FUNCTION miovision_api.create_yyyy_volumes_partition(text, integer, text) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.create_yyyy_volumes_partition(text, integer, text) TO miovision_api_bot;