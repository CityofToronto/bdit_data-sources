SELECT
    date_trunc('day', datetime_15min)::date AS dt,
    COUNT(*) AS count
FROM vds.raw_vdsdata
WHERE
    division_id = 2
    AND datetime_20sec >= {start}::timestamp - INTERVAL {lookback}
    AND datetime_20sec < {start}::timestamp
GROUP BY dt