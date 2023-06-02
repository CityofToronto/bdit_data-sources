--network outages using partition, lag (sarah's method). 
--much faster, runs in 1s for all of rescu. 
--omits outage that is ongoing right now. 
DROP VIEW gwolofs.network_outages;

CREATE VIEW gwolofs.network_outages AS

WITH rescu_summary AS (
    SELECT
        datetime_bin,
        --occasional timeslot where a single sensor reports zero 
        --and no other data. Need to use sum and not count as criteria. 
        COALESCE(SUM(volume_15min), 0) AS volume
    FROM rescu.volumes_15min
    GROUP BY 1
    HAVING COALESCE(SUM(volume_15min), 0) > 0
    ORDER BY 1
),

bin_gaps AS (
    SELECT
        datetime_bin,
        volume,
        datetime_bin - interval '15 MINUTES' AS gap_end,
        CASE
            WHEN
                datetime_bin - LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin) = '00:15:00' THEN 0
            ELSE 1
        END AS bin_break,
        datetime_bin - LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin) AS bin_gap,
        LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin) + interval '15 MINUTES' AS gap_start
    FROM rescu_summary
)

-- calculate the start and end times of gaps that are longer than 15 minutes
SELECT
    gap_start AS time_start,
    gap_end AS time_end,
    (gap_start)::date AS date_start,
    (gap_end)::date AS date_end,
    tsrange(gap_start, gap_end, '[]') AS time_range,
    daterange((gap_start)::date, (gap_end)::date, '[]') AS date_range,
    --no minimum duration for a network wide outage
    EXTRACT(EPOCH FROM gap_end - gap_start) / 86400 AS duration_days
FROM bin_gaps
WHERE
    bin_break = 1
    AND bin_gap IS NOT NULL
ORDER BY datetime_bin;