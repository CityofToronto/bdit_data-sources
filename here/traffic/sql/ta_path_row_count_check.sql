WITH lookback AS ( --noqa: L045
    SELECT
        dt,
        SUM({{ params.col_to_sum }}) AS count,
        ref.is_weekend_or_holiday(dt::date) AS is_weekend_or_holiday
    FROM here.ta_path_daily_summary
    WHERE
        dt >= '{{ ds }}'::date - 1 - interval '{{ params.lookback }}'
        AND dt < '{{ ds }}'::date
)

SELECT
    today.count >= FLOOR(thr.threshold * AVG(lb.count)) AS _check,
    'Daily count: ' || to_char(
        today.count, 'FM9,999,999,999'
    ) AS ds_count,
    initcap('{{ params.lookback }}') || ' Lookback Avg: ' || to_char(
        AVG(lb.count), 'FM9,999,999,999'
    ) AS lookback_avg,
    'Pass threshold: ' || to_char(
        FLOOR(thr.threshold * AVG(lb.count)),
        'FM9,999,999,999'
    ) AS passing_value,
    weather.airport_weather_summary(('{{ ds }}'::date  --noqa: RF01
    - interval '2 days')::date) AS weather_summary
FROM lookback AS today
JOIN lookback AS lb USING (is_weekend_or_holiday),
    LATERAL (
        --change threshold if holiday is not null
        SELECT
            CASE(
                SELECT hol.holiday
                FROM ref.holiday AS hol
                WHERE hol.dt = today.dt
            ) IS NOT NULL
        WHEN False THEN '{{ params.threshold }}'::numeric
        WHEN True THEN 0.5::numeric --50% of weekend volumes for holidays is acceptable
        END
    ) AS thr (threshold)
WHERE
    today.dt = '{{ ds }}'::date - 1
    AND lb.dt != '{{ ds }}'::date - 1
GROUP BY
    today.count,
    thr.threshold