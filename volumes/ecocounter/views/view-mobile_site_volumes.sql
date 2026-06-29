CREATE VIEW ecocounter.mobile_site_volumes AS
SELECT
    su.site_id,
    su.site_description,
    fu.flow_id,
    fu.flow_direction,
    fu.mode_counted,
    cu1.datetime_bin,
    cu1.volume
FROM
    ecocounter.sites_unfiltered AS su
    JOIN ecocounter.flows_unfiltered AS fu USING (site_id)
    JOIN ecocounter.counts_unfiltered AS cu1 USING (flow_id)
WHERE
    site_description LIKE 'Mobile%';

GRANT SELECT ON TABLE ecocounter.mobile_site_volumes TO bdit_humans;

COMMENT ON VIEW ecocounter.mobile_site_volumes
IS 'Data from Ecocounter mobile (pneumatic tube counter) sites.';
