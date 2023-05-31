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
        COALESCE(SUM(volume_15min), 0) as volume
    FROM rescu.volumes_15min
    GROUP BY 1
    HAVING COALESCE(SUM(volume_15min), 0) > 0
    ORDER BY 1
),

bin_gaps AS (
    SELECT 
        datetime_bin,
        datetime_bin - INTERVAL '15 minutes' as gap_end,
        volume,
        CASE
            WHEN datetime_bin - LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin) = '00:15:00' THEN 0
            ELSE 1
        END AS bin_break,
        datetime_bin - LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin) AS bin_gap,
        LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin) + INTERVAL '15 minutes' AS gap_start
    FROM rescu_summary
)

-- calculate the start and end times of gaps that are longer than 15 minutes
SELECT
    bt.gap_start AS time_start,
    bt.gap_end AS time_end,
    tsrange(bt.gap_start, bt.gap_end, '[]') AS time_range,
    (bt.gap_start)::DATE AS date_start,
    (bt.gap_end)::DATE AS date_end,
    daterange((bt.gap_start)::DATE, (bt.gap_end)::DATE, '[]') AS date_range,
    EXTRACT(epoch FROM bt.gap_end - bt.gap_start)/86400 AS duration_days --no minimum duration for a network wide outage
FROM bin_gaps AS bt
WHERE 
    bt.bin_break = 1
    AND bin_gap IS NOT NULL
ORDER BY datetime_bin;