-- FUNCTION: ecocounter.qc_graph_volumes(numeric)

-- DROP FUNCTION IF EXISTS ecocounter.qc_graph_volumes(numeric);

CREATE OR REPLACE FUNCTION ecocounter.qc_graph_volumes(
    site_var numeric
)
RETURNS TABLE (
    site_id numeric,
    flow_id numeric,
    date date,
    daily_volume numeric,
    rolling_avg_1_week numeric,
    flow_color text
)
LANGUAGE 'sql'
COST 100
VOLATILE PARALLEL UNSAFE
ROWS 1000

AS $BODY$

WITH daily_volumes AS (
    SELECT
        site_id,
        f.flow_id,
        datetime_bin::date AS date,
        CASE SUM(lat.calibrated_volume) WHEN 0 THEN null ELSE SUM(lat.calibrated_volume) END AS daily_volume
    FROM ecocounter.counts_unfiltered AS c
    JOIN ecocounter.flows_unfiltered AS f USING (flow_id)
   LEFT JOIN ecocounter.calibration_factors AS cf
     ON c.flow_id = cf.flow_id
     AND c.datetime_bin::date <@ cf.factor_range,
    LATERAL (
      SELECT 
        round(COALESCE(cf.ecocounter_day_corr_factor, 1::numeric) * c.volume::numeric) AS calibrated_volume
    ) lat
    WHERE site_id = site_var
    GROUP BY
        site_id,
        f.flow_id,
        datetime_bin::date
)

SELECT 
    dv.site_id,
    dv.flow_id,
    dv.date,
    dv.daily_volume,
    AVG(dv.daily_volume) OVER w AS rolling_avg_1_week,
    f.flow_direction || ' - ' || f.flow_id AS color
FROM daily_volumes AS dv
LEFT JOIN ecocounter.flows_unfiltered AS f USING (flow_id)
WINDOW w AS (
  PARTITION BY dv.flow_id
  ORDER BY dv.date
  RANGE BETWEEN interval '6 days' PRECEDING AND CURRENT ROW
)
ORDER BY 
    dv.site_id,
    dv.flow_id,
    dv.date

$BODY$;

ALTER FUNCTION ecocounter.qc_graph_volumes(numeric)
OWNER TO ecocounter_admins;

COMMENT ON FUNCTION ecocounter.qc_graph_volumes IS
'A function to get unfiltered flows/volumes for Ecocounter Shiny graphing tool.';