CREATE TABLE ecocounter.counts (
    flow_id numeric REFERENCES ecocounter.locations (flow_id),
    datetime_bin timestamp NOT NULL,
    volume smallint,
    UNIQUE(flow_id, datetime_bin)
);

CREATE INDEX ON ecocounter.counts (datetime_bin);

GRANT SELECT ON ecocounter.counts TO bdit_humans;

COMMENT ON COLUMN ecocounter.counts.datetime_bin
IS 'indicates start time of the time bin. Note that not all time bins are the same size!';