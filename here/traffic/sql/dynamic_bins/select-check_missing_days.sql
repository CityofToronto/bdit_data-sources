WITH distinct_days AS (
    SELECT DISTINCT dt
    FROM gwolofs.congestion_raw_segments
    WHERE
        dt >= '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date --noqa: TMP
        AND dt < '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date + interval '1 month' --noqa: TMP
)

SELECT
    COUNT(*) = 0 AS _check,
    'The following days are missing from `congestion_raw_segments`: '
    || string_agg(dates.dt::date::text, ', ') AS _summary
FROM
    generate_series(
        '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date,
        --one day before start of next month
        ('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date + interval '1 month')::date - 1,
        '1 day'
    ) AS dates (dt)
LEFT JOIN distinct_days USING (dt)
WHERE distinct_days.dt IS NULL;