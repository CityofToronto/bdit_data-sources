-- FUNCTION: bluetooth.create_yyyy_volumes_partition(text, integer, text)

-- DROP FUNCTION IF EXISTS bluetooth.create_yyyy_volumes_partition(text, integer, text);

CREATE OR REPLACE FUNCTION bluetooth.create_yyyy_volumes_partition(
    base_table text,
    year_ integer,
    datetime_col text
)
RETURNS void
LANGUAGE plpgsql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE
    year_table TEXT := base_table||'_'||year_::text;
    startdate DATE := (year_::text || '-01-01')::date;
    enddate DATE := ((year_+1)::text || '-01-01')::date;

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS bluetooth.%1$I
        PARTITION OF bluetooth.%I
        FOR VALUES FROM (%L) TO (%L);
        ALTER TABLE IF EXISTS bluetooth.%1$I OWNER TO bt_admins;
        GRANT SELECT, INSERT, UPDATE ON TABLE bluetooth.%1$I TO events_bot;
        GRANT SELECT, INSERT, UPDATE ON TABLE bluetooth.%1$I TO events_bot;
        GRANT SELECT, REFERENCES ON TABLE bluetooth.%1$I TO bdit_humans;
        $$,
        year_table,
        base_table,
        startdate,
        enddate,
        datetime_col
    );

END;
$BODY$;

ALTER FUNCTION bluetooth.create_yyyy_volumes_partition(text, integer, text)
    OWNER TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.create_yyyy_volumes_partition(text, integer, text) TO PUBLIC;

GRANT EXECUTE ON FUNCTION bluetooth.create_yyyy_volumes_partition(text, integer, text) TO bt_admins;

GRANT EXECUTE ON FUNCTION bluetooth.create_yyyy_volumes_partition(text, integer, text) TO bt_bot;

GRANT EXECUTE ON FUNCTION bluetooth.create_yyyy_volumes_partition(text, integer, text) TO events_bot;

COMMENT ON FUNCTION bluetooth.create_yyyy_volumes_partition(text, integer, text)
    IS '''Create a new year partition under the parent table `base_table`. Only to be used for
bluetooth `itsc_*` table. Use parameter `datetime_col` to specify the partitioning 
timestamp column, ie. `dt`.
Example: `SELECT bluetooth.create_yyyy_volumes_partition(''itsc_tt_raw'', 2024, ''dt'')`''';
