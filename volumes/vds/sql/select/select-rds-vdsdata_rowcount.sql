SELECT
    date_trunc('day', datetime_15min)::date AS dt,
    COUNT(*) AS count
FROM vds.raw_vdsdata
WHERE
    division_id = 2
    AND dt >= {start}::timestamp - interval {lookback}
    AND dt < {start}::timestamp
GROUP BY date_trunc('day', datetime_15min)::date