CREATE OR REPLACE FUNCTION ecocounter.fn_update_last_active()
RETURNS trigger
LANGUAGE 'plpgsql'
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

BEGIN

    WITH new_flow_dates AS (
        SELECT
            flow_id,
            MIN(datetime_bin) AS min_dt,
            MAX(datetime_bin) AS max_dt
        FROM new_rows
        GROUP BY flow_id
    ),
    
    new_site_dates AS (
        SELECT
            site_id,
            MIN(min_dt) AS min_dt,
            MAX(max_dt) AS max_dt
        FROM new_flow_dates
        JOIN ecocounter.flows_unfiltered USING (flow_id)
        GROUP BY site_id
    ),
    
    updated_sites AS (    
        UPDATE ecocounter.sites_unfiltered
        SET
            first_active = LEAST(first_active, min_dt),
            last_active = GREATEST(last_active, max_dt)
        FROM new_site_dates
        WHERE new_site_dates.site_id = sites_unfiltered.site_id
    )
    
    UPDATE ecocounter.flows_unfiltered
    SET
        first_active = LEAST(first_active, min_dt),
        last_active = GREATEST(last_active, max_dt)
    FROM new_flow_dates
    WHERE new_flow_dates.flow_id = flows_unfiltered.flow_id;
    RETURN NULL;
END;
$BODY$;

COMMENT ON FUNCTION ecocounter.fn_update_last_active() IS E''
'This function is called using a trigger after each statement on insert or update to '
'ecocounter.counts_unfiltered. It uses newly inserted/updated rows to update the first_active and'
'last_active columns in ecocounter.sites and ecocounter.flows.';

GRANT EXECUTE ON FUNCTION ecocounter.fn_update_last_active() TO ecocounter_bot;

ALTER FUNCTION ecocounter.fn_update_last_active OWNER TO ecocounter_admins;
