CREATE OR REPLACE VIEW ecocounter.counts AS
SELECT
    counts_unfiltered.flow_id,
    counts_unfiltered.datetime_bin,
    counts_unfiltered.volume AS raw_volume,
    cf.ecocounter_day_corr_factor AS calibration_factor,
    cf.count_date AS validation_date,
    ROUND(COALESCE(cf.ecocounter_day_corr_factor, 1) * counts_unfiltered.volume)
    AS calibrated_volume
FROM ecocounter.counts_unfiltered
JOIN ecocounter.flows_unfiltered USING (flow_id)
JOIN ecocounter.sites_unfiltered USING (site_id)
LEFT JOIN ecocounter.calibration_factors AS cf
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

COMMENT ON VIEW ecocounter.counts
IS 'This view contains the (somewhat) validated counts for Ecocounter flows. 
Counts are only included for sites and flows and marked as validated by a human.
Sites which have undergone validation studies will have `validation_date` and
`calibration_factor` populated to scale raw data to match ground truth (`calibrated_volume`).
Anomalous ranges at the `do-not-use` problem_level are also excluded here.
For the complete, raw count data, see ecocounter.counts_unfiltered. 
Please note that bin size varies for older data, so averaging these numbers may 
not be straightforward.';

ALTER VIEW ecocounter.counts OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.counts TO ecocounter_bot;
GRANT SELECT ON ecocounter.counts TO bdit_humans;

COMMENT ON COLUMN ecocounter.counts.datetime_bin
IS 'indicates start time of the time bin. Note that not all time bins are the same size!';

COMMENT ON COLUMN ecocounter.counts.validation_date
IS 'The date on which a validation study was conducted to determine this calibration factor.';

COMMENT ON COLUMN ecocounter.counts.calibration_factor
IS 'A factor developed through ground-truth validation to scale raw Ecocounter volumes.';
