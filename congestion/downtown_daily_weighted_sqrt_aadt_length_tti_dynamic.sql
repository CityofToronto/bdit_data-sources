--Builds downtown daily congestion metrics by aggregating segment-level tti
--Uses HERE VKT weighting for representative network-level averages
--Separates results by highway, non-highway, and combined networks for better interpretation
--Includes both time-mean and space-mean speeds to capture different performance perspectives
--Provides multiple TTI formulations (rolling, fixed, hybrid) for flexible analysis
--Applies spatial filter (ST_Within) to restrict analysis to core downtown area
CREATE TABLE sborjia.downtown_daily_weighted_length_here_2024_dynamic AS
WITH base AS (
  SELECT
    f.segment_id,
    f.dt,
    EXTRACT(YEAR  FROM f.dt)::int  AS year,
    EXTRACT(MONTH FROM f.dt)::int  AS month,

    -- dynamic MV now gives pm in seconds
    (f.pm_avg_sec / 60.0) AS pm_tt,
    (f.overnight_sec / 60.0) AS overnight_tt,

    f.pm_avg_sec AS pm_time_s,
    f.overnight_sec AS overnight_time_s,

    --TTIs from dynamic MV
    f.tti_rolling_30days,
    f.tti_rolling_4mon,
    f.tti_rolling_6mon,
    f.tti_rolling_annual,
    f.tti_fixed_2024,
    f.tti_fixed_2019,
    f.tti_fixed_2024_monthly,
    f.tti_fixed_2019_monthly,
    f.tti_pre2020_avg2021_ff,
    f.tti_pre2020_10pct_7to21,
    f.tti_pre2020_hw25_nonhw10_post2020_roll6,
    f.tti_pre2020_2021ff_post2020_roll6,

    -- weights 
    sss.vkt_km AS wgt,

    --geometry & validity flags
    NULLIF(g.total_length, 0) AS total_length_m,
    g.highway,
    (NULLIF(g.total_length,0) IS NOT NULL AND f.pm_avg_sec > 0) AS pm_ok,
    (NULLIF(g.total_length,0) IS NOT NULL AND f.overnight_sec > 0) AS ovn_ok,

   --Speeds 
    CASE WHEN f.pm_avg_sec > 0 AND g.total_length > 0
         THEN (g.total_length / f.pm_avg_sec) * 3.6 END AS pm_speed_kmh,

    CASE WHEN f.overnight_sec > 0 AND g.total_length > 0
         THEN (g.total_length / f.overnight_sec) * 3.6 END AS overnight_speed_kmh

  FROM sborjia.citywide_daily_segment_tti_dynamic_dec25 AS f
  JOIN sborjia.segment_here_vkt_2024_final AS sss
    ON f.segment_id = sss.segment_id
  JOIN congestion.network_segments_23_4_geom AS g
    ON f.segment_id = g.segment_id
  WHERE ST_Within(
          ST_Transform(g.geom, 26917),
          (SELECT geom FROM gis.to_core_downtown)
        )
),

expanded AS (
  SELECT b.*, 'highway_true'  AS group_type  FROM base b WHERE b.highway = TRUE
  UNION ALL
  SELECT b.*, 'highway_false' AS group_type  FROM base b WHERE b.highway = FALSE
  UNION ALL
  SELECT b.*, 'both'          AS group_type  FROM base b
)

SELECT
  e.year,
  e.month,
  e.dt,
  e.group_type,

  --SPEED METRICS

  ROUND((
    SUM(e.pm_speed_kmh * e.wgt) FILTER (WHERE e.pm_ok)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.pm_ok), 0)
  )::numeric, 2) AS downtown_pm_avg_speed_kmh,

  ROUND((
    SUM(e.overnight_speed_kmh * e.wgt) FILTER (WHERE e.ovn_ok)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.ovn_ok), 0)
  )::numeric, 2) AS downtown_overnight_avg_speed_kmh,


  ROUND((
    SUM(e.total_length_m * e.wgt) FILTER (WHERE e.pm_ok)
    / NULLIF(SUM(e.pm_time_s * e.wgt) FILTER (WHERE e.pm_ok), 0) * 3.6
  )::numeric, 2) AS downtown_pm_space_mean_speed_kmh,

  ROUND((
    SUM(e.total_length_m * e.wgt) FILTER (WHERE e.ovn_ok)
    / NULLIF(SUM(e.overnight_time_s * e.wgt) FILTER (WHERE e.ovn_ok), 0) * 3.6
  )::numeric, 2) AS downtown_overnight_space_mean_speed_kmh,


  --STANDARD TTI METRICS

  ROUND((
    SUM(e.tti_rolling_30days * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_rolling_30days IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_rolling_30days,

  ROUND((
    SUM(e.tti_rolling_4mon * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_rolling_4mon IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_rolling_4mon,

  ROUND((
    SUM(e.tti_rolling_6mon * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_rolling_6mon IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_rolling_6mon,

  ROUND((
    SUM(e.tti_rolling_annual * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_rolling_annual IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_rolling_annual,

  ROUND((
    SUM(e.tti_fixed_2024 * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_fixed_2024 IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_fixed_2024,

  ROUND((
    SUM(e.tti_fixed_2019 * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_fixed_2019 IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_fixed_2019,

  ROUND((
    SUM(e.tti_fixed_2024_monthly * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_fixed_2024_monthly IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_fixed_2024_monthly,

  ROUND((
    SUM(e.tti_fixed_2019_monthly * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_fixed_2019_monthly IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_fixed_2019_monthly,

 --HYBRID METHODS

  ROUND((
    SUM(e.tti_pre2020_avg2021_ff * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_pre2020_avg2021_ff IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_pre2020_avg2021_ff,

  ROUND((
    SUM(e.tti_pre2020_10pct_7to21 * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_pre2020_10pct_7to21 IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_pre2020_10pct_7to21,

  ROUND((
    SUM(e.tti_pre2020_hw25_nonhw10_post2020_roll6 * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_pre2020_hw25_nonhw10_post2020_roll6 IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_pre2020_hw25_nonhw10_post2020_roll6,

  ROUND((
    SUM(e.tti_pre2020_2021ff_post2020_roll6 * e.wgt)
    / NULLIF(SUM(e.wgt) FILTER (WHERE e.tti_pre2020_2021ff_post2020_roll6 IS NOT NULL), 0)
  )::numeric, 3) AS downtown_tti_pre2020_2021ff_post2020_roll6

FROM expanded e
GROUP BY 1,2,3,4
ORDER BY e.dt, e.group_type;
