CREATE VIEW ecocounter.counts AS (
    SELECT
        counts_unfiltered.flow_id,
        counts_unfiltered.datetime_bin,
        counts_unfiltered.volume
    FROM ecocounter.counts_unfiltered AS 
    JOIN ecocounter.flows USING (flow_id)
    WHERE flows.validated --is true
);

COMMENT ON VIEW ecocounter.counts
IS 'This view contains the actual binned counts for ecocounter flows. Only flows
marked as validated by a human are included here. Please note that bin size varies
for older data, so averaging these numbers may not be straightforward.';

ALTER TABLE ecocounter.counts OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.counts TO ecocounter_bot;
GRANT SELECT ON ecocounter.counts TO bdit_humans;

COMMENT ON COLUMN ecocounter.counts.datetime_bin
IS 'indicates start time of the time bin. Note that not all time bins are the same size!';