DROP VIEW IF EXISTS ecocounter.open_data_daily_counts;
CREATE VIEW ecocounter.open_data_daily_counts AS

SELECT
    s.site_id,
    s.site_description,
    cc.flow_id,
    initcap(f.flow_direction) AS flow_direction,
    cc.datetime_bin::date AS dt,
    SUM(cc.raw_volume) AS raw_volume,
    cc.correction_factor,
    cc.validation_date,
    ROUND(COALESCE(cc.correction_factor, 1) * SUM(cc.raw_volume)) AS corrected_volume
--this view excludes anomalous ranges
FROM ecocounter.counts_corrected AS cc
JOIN ecocounter.flows AS f USING (flow_id)
JOIN ecocounter.sites AS s USING (site_id)
GROUP BY
    s.site_id,
    s.site_description,
    cc.flow_id,
    f.flow_direction,
    cc.datetime_bin::date,
    cc.correction_factor,
    cc.validation_date;

COMMENT ON VIEW ecocounter.open_data_daily_counts IS
'(In development) daily data scaled based on Spectrum studies for Open Data.';

SELECT * FROM ecocounter.open_data_daily_counts WHERE dt >= '2024-10-01';
