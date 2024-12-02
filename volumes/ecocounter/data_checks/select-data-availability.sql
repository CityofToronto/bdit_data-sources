WITH daily_volumes AS (
    SELECT
        dates.dt::date,
        COALESCE(SUM(cu.volume), 0) AS daily_volume
    FROM generate_series('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date, --noqa: PRS
                         '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date --noqa: PRS
                         + '1 month'::interval - '1 day'::interval,
                         '1 day'::interval) AS dates(dt)
    LEFT JOIN ecocounter.counts_unfiltered AS cu ON cu.datetime_bin::date = dates.dt
    WHERE
        cu.datetime_bin >= '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date --noqa: PRS
        AND cu.datetime_bin < '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date --noqa: PRS
            + '1 month'::interval
    GROUP BY dates.dt
    ORDER BY dates.dt
)

SELECT
    NOT(COUNT(*) > 0) AS _check,
    'Missing dates: ' || string_agg(dt::text, ', ') AS summ
FROM daily_volumes
WHERE daily_volume = 0
