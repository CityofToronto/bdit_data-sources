--query to be used in `check_row_count` tasks to compare
--row count to average over lookback period.
--noqa: disable=TMP, PRS

WITH lookback AS ( --noqa: L045
    SELECT
        date_trunc('day', {{ params.dt_col }}) AS _dt, --noqa: L039
        COUNT(DISTINCT {{ params.id_col }}) AS lookback_count, --noqa: L039
        ARRAY_AGG(DISTINCT {{ params.id_col }}) AS daily_ids --noqa: L039
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp
    --group by day then avg excludes missing days.
    GROUP BY _dt --noqa: L003
),

today AS (
    SELECT
        COUNT(DISTINCT {{ params.id_col }}) AS today_count, --noqa: L039
        ARRAY_AGG(DISTINCT {{ params.id_col }}) AS today_ids --noqa: L039
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
),

ids_dif AS (
    SELECT ARRAY_AGG(ids_diff) AS ids_diff
    FROM (
        SELECT DISTINCT UNNEST(lb.daily_ids) AS ids_diff
        FROM lookback AS lb
        EXCEPT
        SELECT UNNEST(a.today_ids)
        FROM today AS a
        ORDER BY ids_diff
    ) AS c
)

SELECT
    a.today_count >= FLOOR({{ params.threshold }}::numeric * AVG(lb.lookback_count)) AS check, 
    'Daily count: ' || to_char(a.today_count, 'FM9,999,999,999') AS ds_count,
    initcap('{{ params.lookback }}') || ' Lookback Avg: '
        || to_char(AVG(lb.lookback_count), 'FM9,999,999,999') AS lookback_avg,
    'Pass threshold: ' || to_char(
            FLOOR({{ params.threshold }}::numeric * AVG(lb.lookback_count)),
            'FM9,999,999,999'
            ) AS passing_value,
    CASE WHEN array_length(c.ids_diff, 1) > 0
        THEN 'The following ' || '{{ params.id_col }}' || ' were present within the last ' || '{{ params.lookback }}' || ', but absent today:' || 
            c.ids_diff::text
        ELSE 'All ids present.'
    END AS id_diff
FROM today AS a,
    lookback AS lb, --noqa: L025
    ids_dif AS c
GROUP BY a.today_count, c.ids_diff