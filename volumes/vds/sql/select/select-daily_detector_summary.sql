SELECT
    c15.detector_id,
    c15.division_id,
    c15.vds_id,
    date_trunc('day', c15.datetime_15min) AS dt,
    c15.num_lanes,
    ROUND(
        SUM(c15.num_obs)::numeric / (c15.expected_bins * c15.num_lanes * 96),
        3) AS obs_percentage_20s,
    ROUND(
        SUM(c15.num_distinct_lanes)::numeric / (c15.num_lanes * 96), 
        3) AS obs_lane_percentage_15min,
    SUM(c15.num_obs) AS num_obs,
    SUM(c15.count_15min) AS count_day
FROM vds.counts_15min AS c15
WHERE c15.division_id = 2
GROUP BY
    c15.detector_id,
    c15.division_id,
    c15.vds_id,
    dt,
    c15.num_lanes,
    c15.expected_bins