WITH lookback AS ( --noqa: L045
    SELECT
        dt,
        SUM({{ params.col_to_sum }}) AS count, --noqa: TMP
        ref.is_weekend_or_holiday(dt::date) AS is_weekend_or_holiday
    FROM here.ta_path_daily_summary
    WHERE
        dt >= '{{ macros.ds_add(ds, -1) }}'::date - interval '{{ params.lookback }}' --noqa: TMP
        AND dt < '{{ ds }}'::date --noqa: TMP
    GROUP BY dt
)

SELECT
    today.count >= FLOOR(thr.threshold * AVG(lb.count)) AS _check,
    'Daily count: ' || to_char(
        today.count, 'FM9,999,999,999'
    ) AS ds_count,
    initcap('{{ params.lookback }}') || ' Lookback Avg: ' || to_char( --noqa: TMP
        AVG(lb.count), 'FM9,999,999,999'
    ) AS lookback_avg,
    'Pass threshold: ' || to_char(
        FLOOR(thr.threshold * AVG(lb.count)),
        'FM9,999,999,999'
    ) AS passing_value,
    weather.airport_weather_summary('{{ macros.ds_add(ds, -1) }}'::date) --noqa: RF01, TMP
    AS weather_summary
FROM lookback AS today
JOIN lookback AS lb USING (is_weekend_or_holiday),
    LATERAL (
        --change threshold if holiday is not null
        SELECT
            CASE (
                SELECT hol.holiday
                FROM ref.holiday AS hol
                WHERE hol.dt = today.dt
            ) IS NOT NULL
                WHEN FALSE THEN '{{ params.threshold }}'::numeric --noqa: TMP
                WHEN TRUE THEN 0.5::numeric --50% of weekend volumes for holidays is acceptable
            END
    ) AS thr (threshold)
WHERE
    today.dt = '{{ macros.ds_add(ds, -1) }}'::date --noqa: TMP
    AND lb.dt != '{{ macros.ds_add(ds, -1) }}'::date --noqa: TMP
GROUP BY
    today.count,
    thr.threshold