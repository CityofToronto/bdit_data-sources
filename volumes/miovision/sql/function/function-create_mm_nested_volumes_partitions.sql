CREATE OR REPLACE FUNCTION miovision_api.create_mm_nested_volumes_partitions(
    base_table text,
    year_ integer,
    mm_ integer
)
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
	month_table TEXT;
    mm_pad TEXT;

BEGIN
    mm_pad:=lpad(mm_::text, 2, '0');
    start_mm:= to_date(year_::text||'-'||mm_pad||'-01', 'YYYY-MM-DD');
    end_mm:= start_mm + INTERVAL '1 month';
    month_table:= year_table||mm_pad;
    EXECUTE FORMAT($$
            CREATE TABLE IF NOT EXISTS miovision_api.%I
            PARTITION OF miovision_api.%I
            FOR VALUES FROM (%L) TO (%L);
            ALTER TABLE IF EXISTS miovision_api.%I OWNER TO miovision_admins;
            GRANT SELECT, REFERENCES ON TABLE miovision_api.%I TO bdit_humans WITH GRANT OPTION;
            GRANT SELECT, INSERT, UPDATE ON TABLE miovision_api.%I TO miovision_api_bot;
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

COMMENT ON FUNCTION miovision_api.create_mm_nested_volumes_partitions(text, integer, integer) IS
'''Create a new month partition under the parent year table `base_table`. Only to be used for
miovision_api `volumes_15min_mvt_unfiltered` table. Example:
`SELECT miovision_api.create_yyyy_volumes_partition(''volumes_15min_mvt_unfiltered'', 2023)`''';

ALTER FUNCTION miovision_api.create_mm_nested_volumes_partitions(text, integer, integer)
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.create_mm_nested_volumes_partitions(text, integer, integer)
TO miovision_api_bot;