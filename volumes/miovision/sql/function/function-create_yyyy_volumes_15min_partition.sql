CREATE OR REPLACE FUNCTION miovision_api.create_yyyy_volumes_15min_partition(
    base_table text,
    year_ integer
)
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
	tablename TEXT;

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS miovision_api.%I
        PARTITION OF miovision_api.%I
        FOR VALUES FROM (%L) TO (%L);
        ALTER TABLE IF EXISTS miovision_api.%I OWNER TO miovision_admins;
        GRANT SELECT, REFERENCES ON TABLE miovision_api.%I TO bdit_humans WITH GRANT OPTION;
        GRANT UPDATE, SELECT, INSERT ON TABLE miovision_api.%I TO miovision_api_bot;
        $$,
        year_table,
        base_table,
        startdate,
        enddate,
        year_table,
        year_table,
        year_table
    );

END;
$BODY$;

COMMENT ON FUNCTION miovision_api.create_yyyy_volumes_15min_partition(text, integer) IS
'''Create a new year partition under the parent table `base_table`. Only to be used for
miovision_api `volumes_15min_mvt_unfiltered` table. Example:
`SELECT miovision_api.create_yyyy_volumes_partition(''volumes_15min_mvt_unfiltered'', 2023)`''';

ALTER FUNCTION miovision_api.create_yyyy_volumes_15min_partition(text, integer)
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.create_yyyy_volumes_15min_partition(text, integer)
TO miovision_api_bot;