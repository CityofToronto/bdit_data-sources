-- Creates sborjia.citywide_monthly_segment_tti_ri_dynamic_dec25 from gwolofs.congestion_segments_monthy_summary
-- Output grain: one row per (segment_id, mnth) where PM (hr 16–19, is_wkdy=TRUE) exists
-- PM stats (pm_avg_sec, pm_p50_sec, pm_p85_sec, pm_p95_sec) are aggregated from gwolofs.congestion_segments_monthy_summary
--   using num_quasi_obs as weights across hr 16–18 (percentiles are weighted approximations)
-- Overnight baselines (overnight_sec) are aggregated from gwolofs.congestion_segments_monthy_summary using:
--   - for mnth < 2024: hr 0–3, is_wkdy=TRUE
--   - for mnth >= 2024: hr 1–4, is_wkdy=TRUE
-- Rolling overnight baselines (roll_1mon_sec, roll_4mon_sec, roll_6mon_sec, roll_annual_sec) are computed over months
--   and exclude the current month (ROWS ... 1 PRECEDING)
-- TTI fields are ratios of PM mean to the selected overnight baseline (rolling / fixed / hybrid)
-- Geometry fields (highway_flag, direction, geom) come from congestion.network_segments_23_4_geom

DROP MATERIALIZED VIEW IF EXISTS sborjia.citywide_monthly_segment_tti_ri_dynamic_dec25;

CREATE MATERIALIZED VIEW sborjia.citywide_monthly_segment_tti_ri_dynamic_dec25 AS
WITH

-- ---------------------------------------------------------
-- Source data:
-- Monthly segment-level summary derived from raw HERE travel times
-- One row per (segment_id, month, weekday flag, hour)
-- Includes mean TT, stdev, percentiles, and quasi-observation counts
-- ---------------------------------------------------------
monthly_src AS MATERIALIZED (
  SELECT
    segment_id,
    mnth::date AS mnth,
    is_wkdy,
    hr,
    avg_tt,
    stdev,
    percentile_05,
    percentile_15,
    percentile_50,
    percentile_85,
    percentile_95,
    num_quasi_obs
  FROM gwolofs.congestion_segments_monthy_summary
  WHERE mnth BETWEEN DATE '2019-01-01' AND DATE '2025-12-01'
),

-- ---------------------------------------------------------
-- Weekday PM peak rows (16–18)
-- Filters to valid weekday PM conditions only
-- ---------------------------------------------------------
pm_rows AS MATERIALIZED (
  SELECT *
  FROM monthly_src
  WHERE is_wkdy = TRUE
    AND hr BETWEEN 16 AND 18
    AND avg_tt IS NOT NULL
    AND avg_tt > 0
),

-- ---------------------------------------------------------
-- Monthly PM statistics per segment
-- Aggregated across hours 16–18 using num_quasi_obs as weights
-- Percentiles are approximated via weighted averaging
-- ---------------------------------------------------------
monthly_pm AS MATERIALIZED (
  SELECT
    segment_id,
    mnth,

    -- Weighted PM mean travel time (seconds)
    (SUM(avg_tt * num_quasi_obs)::double precision
      / NULLIF(SUM(num_quasi_obs), 0)) AS pm_avg_sec,

    -- Approximate weighted percentiles (seconds)
    (SUM(percentile_50 * num_quasi_obs)::double precision
      / NULLIF(SUM(num_quasi_obs), 0)) AS pm_p50_sec,

    (SUM(percentile_85 * num_quasi_obs)::double precision
      / NULLIF(SUM(num_quasi_obs), 0)) AS pm_p85_sec,

    (SUM(percentile_95 * num_quasi_obs)::double precision
      / NULLIF(SUM(num_quasi_obs), 0)) AS pm_p95_sec

  FROM pm_rows
  GROUP BY segment_id, mnth
),

-- ---------------------------------------------------------
-- Overnight rows used to define free-flow baselines
-- Hour windows change in 2024 to reflect data availability
-- ---------------------------------------------------------
overnight_rows AS MATERIALIZED (
  SELECT
    segment_id,
    mnth,
    avg_tt,
    num_quasi_obs
  FROM monthly_src
  WHERE is_wkdy = TRUE
    AND avg_tt IS NOT NULL
    AND avg_tt > 0
    AND (
      (EXTRACT(YEAR FROM mnth) < 2024 AND hr BETWEEN 0 AND 3)
      OR
      (EXTRACT(YEAR FROM mnth) >= 2024 AND hr BETWEEN 1 AND 4)
    )
),

-- ---------------------------------------------------------
-- Monthly overnight average per segment
-- Used as the base for rolling and fixed free-flow baselines
-- ---------------------------------------------------------
overnight_monthly AS MATERIALIZED (
  SELECT
    segment_id,
    mnth,
    (SUM(avg_tt * num_quasi_obs)::double precision
      / NULLIF(SUM(num_quasi_obs), 0)) AS overnight_monthly_avg_sec
  FROM overnight_rows
  GROUP BY segment_id, mnth
),

-- ---------------------------------------------------------
-- PM calendar
-- Ensures months with PM data are retained even if overnight is missing
-- ---------------------------------------------------------
pm_calendar AS MATERIALIZED (
  SELECT DISTINCT segment_id, mnth
  FROM monthly_pm
),

-- ---------------------------------------------------------
-- Align overnight values to PM months
-- ---------------------------------------------------------
overnight_on_pm AS MATERIALIZED (
  SELECT
    pm.segment_id,
    pm.mnth,
    om.overnight_monthly_avg_sec
  FROM pm_calendar pm
  LEFT JOIN overnight_monthly om
    ON om.segment_id = pm.segment_id
   AND om.mnth       = pm.mnth
),

-- ---------------------------------------------------------
-- Rolling overnight baselines (month-based windows)
-- Current month is excluded to avoid look-ahead bias
-- ---------------------------------------------------------
rolling_1mon AS MATERIALIZED (
  SELECT
    segment_id,
    mnth,
    AVG(overnight_monthly_avg_sec) OVER (
      PARTITION BY segment_id
      ORDER BY mnth
      ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
    ) AS roll_1mon_sec
  FROM overnight_on_pm
),

rolling_4mon AS MATERIALIZED (
  SELECT
    segment_id,
    mnth,
    AVG(overnight_monthly_avg_sec) OVER (
      PARTITION BY segment_id
      ORDER BY mnth
      ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
    ) AS roll_4mon_sec
  FROM overnight_on_pm
),

rolling_6mon AS MATERIALIZED (
  SELECT
    segment_id,
    mnth,
    AVG(overnight_monthly_avg_sec) OVER (
      PARTITION BY segment_id
      ORDER BY mnth
      ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
    ) AS roll_6mon_sec
  FROM overnight_on_pm
),

rolling_annual AS MATERIALIZED (
  SELECT
    segment_id,
    mnth,
    AVG(overnight_monthly_avg_sec) OVER (
      PARTITION BY segment_id
      ORDER BY mnth
      ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
    ) AS roll_annual_sec
  FROM overnight_on_pm
),

-- ---------------------------------------------------------
-- Fixed overnight baselines (annual and monthly)
-- Used for comparison and historical consistency
-- ---------------------------------------------------------
fixed_annual_2019 AS MATERIALIZED (
  SELECT
    segment_id,
    AVG(overnight_monthly_avg_sec) AS fixed_2019_sec
  FROM overnight_monthly
  WHERE EXTRACT(YEAR FROM mnth) = 2019
  GROUP BY segment_id
),

fixed_annual_2024 AS MATERIALIZED (
  SELECT
    segment_id,
    AVG(overnight_monthly_avg_sec) AS fixed_2024_sec
  FROM overnight_monthly
  WHERE EXTRACT(YEAR FROM mnth) = 2024
  GROUP BY segment_id
),

fixed_monthly_2019 AS MATERIALIZED (
  SELECT
    segment_id,
    EXTRACT(MONTH FROM mnth)::int AS mon,
    AVG(overnight_monthly_avg_sec) AS fixed_2019_monthly_sec
  FROM overnight_monthly
  WHERE EXTRACT(YEAR FROM mnth) = 2019
  GROUP BY segment_id, mon
),

fixed_monthly_2024 AS MATERIALIZED (
  SELECT
    segment_id,
    EXTRACT(MONTH FROM mnth)::int AS mon,
    AVG(overnight_monthly_avg_sec) AS fixed_2024_monthly_sec
  FROM overnight_monthly
  WHERE EXTRACT(YEAR FROM mnth) = 2024
  GROUP BY segment_id, mon
),

-- ---------------------------------------------------------
-- Fixed 2021 overnight baseline
-- Used as free-flow reference for pre-2020 hybrid TTI logic
-- ---------------------------------------------------------
avg_overnight_2021 AS MATERIALIZED (
  SELECT
    segment_id,
    AVG(overnight_monthly_avg_sec) AS avg_2021_overnight_ff
  FROM overnight_monthly
  WHERE EXTRACT(YEAR FROM mnth) = 2021
  GROUP BY segment_id
),

-- ---------------------------------------------------------
-- Final output:
-- One row per segment per month
-- Includes PM stats, overnight baselines, multiple TTI variants,
-- and geometry for spatial joins
-- ---------------------------------------------------------
final AS (
  SELECT
    pm.segment_id,
    pm.mnth,
    EXTRACT(YEAR FROM pm.mnth)::int  AS year,
    EXTRACT(MONTH FROM pm.mnth)::int AS month,

    -- PM travel time metrics (seconds)
    pm.pm_avg_sec,
    pm.pm_p50_sec,
    pm.pm_p85_sec,
    pm.pm_p95_sec,

    -- Monthly overnight average (seconds)
    od.overnight_monthly_avg_sec AS overnight_sec,

    -- Exposed rolling overnight baseline
    r6.roll_6mon_sec AS overnight_roll_6mon_sec,

    -- Rolling TTI variants
    pm.pm_avg_sec / r1.roll_1mon_sec    AS tti_rolling_30days,
    pm.pm_avg_sec / r4.roll_4mon_sec    AS tti_rolling_4mon,
    pm.pm_avg_sec / r6.roll_6mon_sec    AS tti_rolling_6mon,
    pm.pm_avg_sec / r12.roll_annual_sec AS tti_rolling_annual,

    -- Fixed-baseline TTI variants
    pm.pm_avg_sec / fa19.fixed_2019_sec         AS tti_fixed_2019,
    pm
