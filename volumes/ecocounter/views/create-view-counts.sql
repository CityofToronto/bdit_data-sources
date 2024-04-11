CREATE VIEW ecocounter.counts AS (
    SELECT flow_id, datetime_bin, volume
    FROM ecocounter.counts_unfiltered
    JOIN ecocounter.flows USING (flow_id)
    WHERE flows.validated --is true
)

COMMENT ON VIEW ecocounter.counts
IS 'This view contains the actual binned counts for ecocounter flows. Only flows
marked as validated by a human are included here. Please note that bin size varies
for older data, so averaging these numbers may not be straightforward.';
