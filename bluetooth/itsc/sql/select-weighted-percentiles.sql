--establish percentiles weighted by sample size
WITH weighted_data AS (
    SELECT
        path_id,
        date_trunc('hour', dt) AS hr,
        travel_time_s AS mean,
        SUM(num_samples) AS n,
        SUM(SUM(num_samples)) OVER (PARTITION BY path_id, date_trunc('hour', dt)) AS total_sample,
        --sum of samples which have appeared below that speed
        SUM(SUM(num_samples)) OVER (PARTITION BY path_id, date_trunc('hour', dt) ORDER BY travel_time_s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        -- / total_sample
         / SUM(SUM(num_samples)) OVER (PARTITION BY path_id, date_trunc('hour', dt)) AS record_frac
    FROM gwolofs.tt_raw
    WHERE path_id = 6759128 AND dt >= '2025-01-01' --date_trunc('hour', dt) = '2025-02-22 14:00'
    GROUP BY
        path_id,
        hr,
        travel_time_s
    ORDER BY
        path_id,
        hr,
        SUM(SUM(num_samples)) OVER (PARTITION BY path_id, date_trunc('hour', dt) ORDER BY travel_time_s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)

--take the first value that exceeds 85% (as in percentile_disc)
SELECT
    path_id,
    hr,
    ROUND(AVG(mean), 0) AS unweighted_avg,
    ROUND(SUM(mean*n/total_sample), 0) AS weighted_avg,
    MIN(mean) FILTER (WHERE record_frac >= 0.15) AS percentile_15th,
    MIN(mean) FILTER (WHERE record_frac >= 0.50) AS percentile_50th,
    MIN(mean) FILTER (WHERE record_frac >= 0.85) AS percentile_85th
FROM weighted_data
GROUP BY
    path_id,
    hr
ORDER BY
    path_id,
    hr