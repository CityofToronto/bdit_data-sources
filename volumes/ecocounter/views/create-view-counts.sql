CREATE OR REPLACE VIEW ecocounter.counts AS
SELECT
    counts_unfiltered.flow_id,
    counts_unfiltered.datetime_bin,
    counts_unfiltered.volume
FROM ecocounter.counts_unfiltered
JOIN ecocounter.flows_unfiltered USING (flow_id)
JOIN ecocounter.sites_unfiltered USING (site_id)
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
            AND (
                (
                    -- flow has a problem is this time range
                    counts_unfiltered.flow_id = anomalous_ranges.flow_id
                    AND anomalous_ranges.time_range @> counts_unfiltered.datetime_bin
                ) OR (
                    -- site has a problem in this time range
                    sites_unfiltered.site_id = anomalous_ranges.site_id
                    AND anomalous_ranges.time_range @> counts_unfiltered.datetime_bin
                )
            )
    );

COMMENT ON VIEW ecocounter.counts
IS 'This view contains the (somewhat) validated counts for Ecocounter flows. 
Counts are only included for sites and flows and marked as validated by a human.
Anomalous ranges at the do-not-use problem_level are also excluded here.
For the complete, raw count data, see ecocounter.counts_unfiltered. 
Please note that bin size varies for older data, so averaging these numbers may 
not be straightforward.';

ALTER VIEW ecocounter.counts OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.counts TO ecocounter_bot;
GRANT SELECT ON ecocounter.counts TO bdit_humans;

COMMENT ON COLUMN ecocounter.counts.datetime_bin
IS 'indicates start time of the time bin. Note that not all time bins are the same size!';
