--runs in 23s for 2023. 

CREATE VIEW gwolofs.network_outages AS

--continuous outages
WITH time_bins AS (
    SELECT generate_series(
        '2023-01-01 00:00'::TIMESTAMP,
        '2023-05-28 23:45',
        '15 minutes'
        ) AS time_bin
), 

network_bins AS (
    SELECT
        tb.time_bin,
        --occasional timeslot where a single sensor reports zero 
            --and no other data. Need to use sum and not count as criteria. 
        COALESCE(SUM(v15.volume_15min), 0) > 0 AS network_active --coalesce null to zero
    FROM time_bins AS tb
    LEFT JOIN rescu.volumes_15min AS v15 ON v15.datetime_bin = tb.time_bin
    GROUP BY 1
    ORDER BY 1
),

run_groups AS (
    SELECT
        time_bin,
        network_active,
        (
            SELECT COUNT(*)
            FROM network_bins AS g
            WHERE g.network_active <> gr.network_active --state change from active to inactive (or reverse)
                AND g.time_bin <= gr.time_bin --running total
        ) AS run_group
    FROM network_bins AS gr
)

SELECT
    MIN(time_bin) AS time_start,
    MAX(time_bin) AS time_end,
    tsrange(MIN(time_bin), MAX(time_bin), '[]') AS time_range,
    MIN(time_bin)::DATE AS date_start,
    MAX(time_bin)::DATE AS date_end,
    daterange(MIN(time_bin)::DATE, MAX(time_bin)::DATE, '[]') AS date_range,
    COUNT(*) / (60.0 / 15 * 24) AS duration_days --no minimum duration for a network wide outage
FROM run_groups
WHERE network_active = FALSE --only count length of outages
GROUP BY run_group
ORDER BY MIN(time_bin);