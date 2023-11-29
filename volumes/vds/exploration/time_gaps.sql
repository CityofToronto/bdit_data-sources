--sensors in the same vdsdata table have different time gaps, ie. some 20sec, some 5 min. 
--No attribute present in vdsconfig.
--Determine using top bin present over a month. 
--17 minutes to run.

WITH time_gaps AS (
    SELECT
        d.vds_id,
        d.lane,
        c.detector_id,
        d.dt - lag(d.dt, 1) OVER (
            PARTITION BY d.vds_id, d.lane ORDER BY d.dt
        ) AS gap
    FROM vds.raw_vdsdata AS d
    LEFT JOIN vds.vdsconfig AS c ON
        d.vds_id = c.vds_id
        AND d.division_id = c.division_id
        AND d.dt >= c.start_timestamp
        AND (
            d.dt <= c.end_timestamp
            OR c.end_timestamp IS NULL) --no end date
    WHERE
        --filter to specific sensors here!
        --change dates to recent period
        d.dt >= '2023-05-01 00:00:00'::timestamp
        AND d.dt < '2023-06-01 00:00:00'::timestamp
), 

gap_count AS (
    SELECT
        vds_id,
        detector_id,
        gap,
        COUNT(*) AS count
    FROM time_gaps
    GROUP BY
        vds_id,
        detector_id,
        gap
)

SELECT DISTINCT ON (vds_id)
    vds_id,
    detector_id,
    gap,
    count
FROM gap_count
ORDER BY
    vds_id ASC,
    count DESC --top count using distinct on