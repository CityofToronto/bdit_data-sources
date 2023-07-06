--sensors in the same vdsdata table have different time gaps, ie. some 20sec, some 5 min. 
--No attribute present in 

WITH time_gaps AS (
    SELECT
        vds_id,
        lane,
        datetime_20sec - lag(datetime_20sec, 1) OVER (
            PARTITION BY vds_id, lane ORDER BY datetime_20sec
        ) AS gap
    FROM vds.raw_vdsdata
)

SELECT DISTINCT ON (vds_id)
    vds_id,
    detector_id,
    c.start_timestamp,
    gap,
    COUNT(*)
FROM time_gaps
LEFT JOIN vds.vdsconfig AS c USING (vds_id)
GROUP BY
    vds_id,
    detector_id,
    c.start_timestamp,
    gap
ORDER BY
    vds_id ASC,
    COUNT(*) DESC,
    c.start_timestamp DESC
