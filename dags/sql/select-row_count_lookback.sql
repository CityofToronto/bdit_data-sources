--query to be used in `check_row_count` tasks to compare
--row count to average over lookback period.
--noqa: disable=TMP, PRS

WITH lookback AS ( --noqa: L045
    SELECT
        {{ params.dt_col }}::date AS _dt, --noqa: L039
        SUM({{ params.col_to_sum }}) AS count,
		ref.is_weekend_or_holiday({{ params.dt_col }}::date) AS is_weekend_or_holiday
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
    --group by day then avg excludes missing days.
    GROUP BY _dt --noqa: L003
)

SELECT
    today.count >= FLOOR(thr.threshold * AVG(lb.count)) AS check, 
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
    weather.airport_weather_summary('{{ ds }}') AS weather_summary
FROM lookback AS today
JOIN lookback AS lb USING (is_weekend_or_holiday),
    LATERAL(
        --change threshold if holiday is not null
        SELECT CASE (SELECT hol.holiday FROM ref.holiday AS hol WHERE hol.dt = today._dt) IS NOT NULL
            WHEN False THEN {{ params.threshold }}::numeric
            WHEN True THEN 0.5::numeric --50% of weekend volumes for holidays is acceptable
        END
    ) AS thr (threshold)
WHERE
    today._dt = '{{ ds }}'::date
    AND lb._dt != '{{ ds }}'::date
GROUP BY
    today.count,
    thr.threshold