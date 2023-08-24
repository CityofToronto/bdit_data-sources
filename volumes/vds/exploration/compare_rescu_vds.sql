WITH vds_count AS (
    SELECT
        v.datetime_15min::date,
        COUNT(v.*) AS vds_count,
        SUM(v.count_15min) AS vds_sum
    FROM vds.counts_15min AS v
    WHERE
        v.division_id = 2
        AND v.datetime_15min >= '2023-01-01 00:00:00'
        --AND v.datetime_15min < '2023-01-01 00:00:00'
    GROUP BY v.datetime_15min::date
)

SELECT
    v.datetime_15min,
    COUNT(r.*) AS rescu_count,
    SUM(r.volume_15min) AS rescu_sum,
    v.vds_count,
    v.vds_sum
FROM rescu.volumes_15min AS r
RIGHT JOIN vds_count AS v ON
    r.datetime_bin::date = v.datetime_15min
GROUP BY
    v.datetime_15min,
    v.vds_count,
    v.vds_sum
ORDER BY 
    v.datetime_15min
    
