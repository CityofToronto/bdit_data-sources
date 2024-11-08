DROP VIEW IF EXISTS ecocounter.open_data_daily_counts;
CREATE VIEW ecocounter.open_data_daily_counts AS

SELECT
    s.site_id,
    s.site_description,
    f.direction_main AS direction,
    cc.datetime_bin::date AS dt,
    SUM(corrected_volume) AS daily_volume
--this view excludes anomalous ranges
FROM ecocounter.counts_corrected AS cc
JOIN ecocounter.flows AS f USING (flow_id)
JOIN ecocounter.sites AS s USING (site_id)
GROUP BY
    s.site_id,
    s.site_description,
    f.direction_main,
    cc.datetime_bin::date
HAVING SUM(corrected_volume) > 0;

COMMENT ON VIEW ecocounter.open_data_daily_counts IS
'(In development) daily data scaled based on Spectrum studies for Open Data.';

ALTER TABLE ecocounter.open_data_daily_counts OWNER TO ecocounter_admins;
GRANT SELECT ON TABLE ecocounter.open_data_daily_counts TO bdit_humans WITH GRANT OPTION;

GRANT SELECT ON TABLE ecocounter.open_data_daily_counts TO ecocounter_bot;