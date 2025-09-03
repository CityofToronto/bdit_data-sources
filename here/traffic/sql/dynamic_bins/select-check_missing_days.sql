WITH distinct_days AS (
    SELECT DISTINCT dt
    FROM gwolofs.congestion_raw_segments
    WHERE
        dt >= '{{ ds }}'::date
        AND dt < '{{ ds }}'::date + interval '1 month'
)

SELECT
    COUNT(*) = 0,
    'The following days are missing from `congestion_raw_segments`: '
    || string_agg(dates.dt::date::text, ', ') AS summary
FROM generate_series(
    '{{ ds }}'::date,
    --one day before start of next month
    ('{{ ds }}'::date + interval '1 month')::date - 1,
    '1 day') AS dates(dt)
LEFT JOIN distinct_days USING (dt)
WHERE distinct_days.dt IS NULL;