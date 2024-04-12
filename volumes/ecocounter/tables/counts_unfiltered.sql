CREATE TABLE ecocounter.counts_unfiltered (
    flow_id numeric NOT NULL,
    datetime_bin timestamp without time zone NOT NULL,
    volume smallint,
    CONSTRAINT counts_unfiltered_flow_id_datetime_bin_key UNIQUE (flow_id, datetime_bin),
    CONSTRAINT counts_flow_id_fkey FOREIGN KEY (flow_id)
    REFERENCES ecocounter.flows (flow_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
) PARTITION BY RANGE (datetime_bin);

CREATE INDEX IF NOT EXISTS counts_unfiltered_flow_id_idx
ON ecocounter.counts_unfiltered USING btree (flow_id ASC NULLS LAST);

CREATE INDEX IF NOT EXISTS counts_unfiltered_datetime_bin_idx
ON ecocounter.counts_unfiltered USING btree (datetime_bin ASC NULLS LAST);

ALTER TABLE ecocounter.counts_unfiltered OWNER TO ecocounter_admins;

GRANT SELECT, INSERT, DELETE ON ecocounter.counts_unfiltered TO ecocounter_bot;

REVOKE ALL ON TABLE ecocounter.counts_unfiltered FROM bdit_humans;

COMMENT ON TABLE ecocounter.counts_unfiltered
IS 'CAUTION: Use VIEW `ecocounter.counts` instead to see data only for sites verified by a human.
This Table contains the actual binned counts for ecocounter flows. Please note that
bin size varies for older data, so averaging these numbers may not be straightforward.';

COMMENT ON COLUMN ecocounter.counts_unfiltered.datetime_bin
IS 'indicates start time of the time bin. Note that not all time bins are the same size!';