CREATE TABLE ecocounter.counts (
    flow_id numeric REFERENCES ecocounter.locations (flow_id),
    datetime_bin timestamp NOT NULL,
    volume smallint,
    UNIQUE(flow_id, datetime_bin)
);

CREATE INDEX ON ecocounter.counts (flow_id);

CREATE INDEX ON ecocounter.counts (datetime_bin);

GRANT SELECT ON ecocounter.counts TO bdit_humans;

ALTER TABLE ecocounter.counts OWNER TO ecocounter_admins;

COMMENT ON TABLE ecocounter.counts
IS 'Table contains the actual binned counts for ecocounter flows. Please note that bin size varies for older data, so averaging these numbers may not be straightforward.';

COMMENT ON COLUMN ecocounter.counts.datetime_bin
IS 'indicates start time of the time bin. Note that not all time bins are the same size!';