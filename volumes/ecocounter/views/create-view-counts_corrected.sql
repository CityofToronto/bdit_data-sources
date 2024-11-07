CREATE OR REPLACE VIEW ecocounter.counts_corrected AS
SELECT
    counts_unfiltered.flow_id,
    counts_unfiltered.datetime_bin,
    counts_unfiltered.volume AS raw_volume,
    cf.ecocounter_day_corr_factor AS correction_factor,
    cf.count_date AS validation_date,
    ROUND(COALESCE(cf.ecocounter_day_corr_factor, 1) * counts_unfiltered.volume) AS corrected_volume
FROM ecocounter.counts_unfiltered
JOIN ecocounter.flows_unfiltered USING (flow_id)
JOIN ecocounter.sites_unfiltered USING (site_id)
LEFT JOIN ecocounter.correction_factors AS cf
    ON
    counts_unfiltered.flow_id = cf.flow_id
    AND counts_unfiltered.datetime_bin::date <@ cf.factor_range
WHERE
    -- must be validated at the level of both site and flow
    flows_unfiltered.validated
    AND sites_unfiltered.validated
    -- must not intersect with any do-not-use level anomalous ranges
    AND NOT EXISTS (
        SELECT 1
        FROM ecocounter.anomalous_ranges
        WHERE
            anomalous_ranges.problem_level = 'do-not-use'
            AND anomalous_ranges.time_range @> counts_unfiltered.datetime_bin
            AND (
                counts_unfiltered.flow_id = anomalous_ranges.flow_id
                OR (
                    anomalous_ranges.flow_id IS NULL
                    AND sites_unfiltered.site_id = anomalous_ranges.site_id
                )
            )
    );

COMMENT ON VIEW ecocounter.counts_corrected
IS '(In development) Ecocounter counts corrected based on Spectrum validation studies.';

ALTER VIEW ecocounter.counts_corrected OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.counts_corrected TO ecocounter_bot;
GRANT SELECT ON ecocounter.counts_corrected TO bdit_humans;

COMMENT ON COLUMN ecocounter.counts_corrected.datetime_bin
IS 'indicates start time of the time bin. Note that not all time bins are the same size!';