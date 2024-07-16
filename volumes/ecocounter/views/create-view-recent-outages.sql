CREATE VIEW ecocounter.recent_outages AS (

    WITH ongoing_outages AS (
        SELECT
            f.flow_id,
            f.site_id,
            dates.dt,
            dates.dt - lag(dates.dt) OVER w = interval '1 day' AS consecutive
        FROM ecocounter.flows_unfiltered AS f
        CROSS JOIN generate_series(
            now()::date - interval '16 days',
            now()::date - interval '2 day',
            interval '1 day'
        ) AS dates(dt)
        LEFT JOIN ecocounter.counts_unfiltered AS c ON
            c.flow_id = f.flow_id
            AND c.datetime_bin >= dates.dt
            AND c.datetime_bin < dates.dt + interval '1 day'
            --select counts partitions
            AND c.datetime_bin >= now()::date - interval '60 days'
            AND c.datetime_bin < now()::date
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
        HAVING
            COALESCE(SUM(c.volume), 0) = 0 --null or zero
        WINDOW w AS (PARTITION BY f.flow_id ORDER BY dates.dt)
        ORDER BY
            f.flow_id,
            dates.dt
    ),
    
    group_ids AS (
        SELECT
            flow_id,
            site_id,
            dt,
            SUM(CASE WHEN consecutive IS True THEN 0 ELSE 1 END) OVER w AS group_id
        FROM ongoing_outages
        WINDOW w AS (PARTITION BY flow_id ORDER BY dt)
    )
    
    SELECT
        flow_id,
        site_id,
        MIN(dt) AS date_start,
        MAX(dt) + interval '1 day' AS date_end
    FROM group_ids
    GROUP BY
        flow_id,
        site_id,
        group_id
);

ALTER VIEW ecocounter.recent_outages OWNER TO ecocounter_admins;
GRANT ALL ON TABLE ecocounter.recent_outages TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.recent_outages FROM bdit_humans;
GRANT SELECT ON TABLE ecocounter.recent_outages TO bdit_humans;

GRANT SELECT ON TABLE ecocounter.recent_outages TO ecocounter_bot;

COMMENT ON VIEW ecocounter.recent_outages
IS 'A view to identify recent outages in Ecocounter data.';