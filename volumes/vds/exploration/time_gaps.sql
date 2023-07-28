--sensors in the same vdsdata table have different time gaps, ie. some 20sec, some 5 min. 
--No attribute present in 

WITH time_gaps AS (
    SELECT
        vds_id,
        lane,
        dt - lag(dt, 1) OVER (
            PARTITION BY vds_id, lane ORDER BY dt
        ) AS gap
    FROM vds.raw_vdsdata
)

SELECT DISTINCT ON (t.vds_id)
    t.vds_id,
    c.detector_id,
    c.start_timestamp,
    t.gap,
    COUNT(*) AS count
FROM time_gaps AS t 
LEFT JOIN vds.vdsconfig AS c USING (vds_id)
GROUP BY
    t.vds_id,
    c.detector_id,
    c.start_timestamp,
    t.gap
ORDER BY
    t.vds_id ASC,
    count DESC,
    c.start_timestamp DESC
