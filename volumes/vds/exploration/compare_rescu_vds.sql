--difference in row counts on good days is from additional sensors
--in vds schema and inclusion of nulls. 
WITH vds_count AS (
    SELECT
        date_trunc('day', datetime_15min) AS dt,
        COUNT(*) AS vds_count,
        SUM(count_15min) AS vds_sum
    FROM vds.counts_15min
    WHERE
        division_id = 2
        AND datetime_15min >= '2023-04-01 00:00:00'
        --AND datetime_15min < '2023-01-01 00:00:00' -- noqa: L003
    GROUP BY dt
)

SELECT
    v.dt,
    COUNT(r.*) AS rescu_count,
    SUM(r.volume_15min) AS rescu_sum,
    v.vds_count,
    v.vds_sum
FROM rescu.volumes_15min AS r
RIGHT JOIN vds_count AS v ON date_trunc('day', r.datetime_bin) = v.dt
GROUP BY
    v.dt,
    v.vds_count,
    v.vds_sum
ORDER BY v.dt