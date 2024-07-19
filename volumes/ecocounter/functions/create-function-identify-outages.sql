CREATE OR REPLACE FUNCTION ecocounter.identify_outages(
    num_days interval
)
RETURNS TABLE (
    flow_id numeric,
    date_start date,
    date_end date
)
LANGUAGE plpgsql
COST 100
VOLATILE

AS $BODY$

BEGIN

RETURN QUERY
WITH ongoing_outages AS (
    SELECT
        f.flow_id,
        f.site_id,
        dates.dt::date,
        dates.dt - lag(dates.dt) OVER w = interval '1 day' AS consecutive
    FROM ecocounter.flows_unfiltered AS f
    CROSS JOIN
        generate_series(
            now()::date - num_days,
            now()::date - interval '2 day', --2 bc last interval will be this + 1 day
            interval '1 day'
        ) AS dates (dt)
    LEFT JOIN ecocounter.counts_unfiltered AS c
        ON c.flow_id = f.flow_id
        AND c.datetime_bin >= dates.dt
        AND c.datetime_bin < dates.dt + interval '1 day'
        --select counts partitions
        AND c.datetime_bin >= now()::date - num_days
        AND c.datetime_bin < now()::date - interval '1 day'
    WHERE
        f.validated
        AND dates.dt < COALESCE(f.date_decommissioned, now()::date - interval '1 day')
    GROUP BY
        f.flow_id,
        f.site_id,
        f.validated,
        f.last_active,
        f.date_decommissioned,
        dates.dt
    HAVING SUM(c.volume) IS NULL
    WINDOW w AS (PARTITION BY f.flow_id ORDER BY dates.dt)
    ORDER BY
        f.flow_id,
        dates.dt
),

group_ids AS (
    SELECT
        oo.flow_id,
        oo.dt,
        SUM(CASE WHEN oo.consecutive IS TRUE THEN 0 ELSE 1 END) OVER w AS group_id
    FROM ongoing_outages AS oo
    WINDOW w AS (PARTITION BY oo.flow_id ORDER BY oo.dt)
)

SELECT
    gi.flow_id,
    MIN(gi.dt)::date AS date_start,
    (MAX(gi.dt) + interval '1 day')::date AS date_end
FROM group_ids AS gi
GROUP BY
    gi.flow_id,
    gi.group_id;

END;
$BODY$;

ALTER FUNCTION ecocounter.identify_outages(interval) OWNER TO ecocounter_admins;
GRANT ALL ON FUNCTION ecocounter.identify_outages(interval) TO ecocounter_admins;

GRANT EXECUTE ON FUNCTION ecocounter.identify_outages(interval) TO bdit_humans;
GRANT EXECUTE ON FUNCTION ecocounter.identify_outages(interval) TO ecocounter_bot;

COMMENT ON FUNCTION ecocounter.identify_outages(interval)
IS 'A function to identify day level outages (null volume) in Ecocounter data and group
them into runs for ease of pulling. Used by Airflow ecocounter_pull.pull_recent_outages task.';