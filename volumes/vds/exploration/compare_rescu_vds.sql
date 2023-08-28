--difference in row counts on good days is from additional sensors
--in vds schema and inclusion of nulls. 
WITH vds_count AS (
    SELECT
        date_trunc('day', datetime_15min) AS day,
        COUNT(v.*) AS vds_count,
        SUM(v.count_15min) AS vds_sum
    FROM vds.counts_15min AS v
    WHERE
        v.division_id = 2
        AND v.datetime_15min >= '2023-04-01 00:00:00'
        --AND v.datetime_15min < '2023-01-01 00:00:00'
    GROUP BY day
)

SELECT
    v.day,
    COUNT(r.*) AS rescu_count,
    SUM(r.volume_15min) AS rescu_sum,
    v.vds_count,
    v.vds_sum
FROM rescu.volumes_15min AS r
RIGHT JOIN vds_count AS v ON date_trunc('day', r.datetime_bin) = v.day
GROUP BY
    v.day,
    v.vds_count,
    v.vds_sum
ORDER BY v.day