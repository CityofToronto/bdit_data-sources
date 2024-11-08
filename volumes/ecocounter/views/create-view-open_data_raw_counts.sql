DROP VIEW IF EXISTS ecocounter.open_data_raw_counts;
CREATE VIEW ecocounter.open_data_raw_counts AS

SELECT
    s.site_id,
    s.site_description,
    f.direction_main AS direction,
    cc.datetime_bin AS datetime_bin,
    SUM(cc.corrected_volume) AS bin_volume
--this view excludes anomalous ranges
FROM ecocounter.counts_corrected AS cc
JOIN ecocounter.flows AS f USING (flow_id)
JOIN ecocounter.sites AS s USING (site_id)
GROUP BY
    s.site_id,
    s.site_description,
    f.direction_main,
    cc.datetime_bin
HAVING SUM(cc.corrected_volume) > 0;

COMMENT ON VIEW ecocounter.open_data_raw_counts IS
'(In development) disaggregate Ecocounter data scaled based on Spectrum studies for Open Data.';

ALTER TABLE ecocounter.open_data_raw_counts OWNER TO ecocounter_admins;
GRANT SELECT ON TABLE ecocounter.open_data_raw_counts TO bdit_humans WITH GRANT OPTION;

GRANT SELECT ON TABLE ecocounter.open_data_raw_counts TO ecocounter_bot;