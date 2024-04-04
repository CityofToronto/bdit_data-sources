CREATE TABLE gwolofs.counts (
    flow_id numeric,
    datetime_bin timestamp without time zone NOT NULL,
    volume smallint,
    CONSTRAINT counts_flow_id_datetime_bin_key UNIQUE (flow_id, datetime_bin),
    CONSTRAINT counts_flow_id_fkey FOREIGN KEY (flow_id)
        REFERENCES gwolofs.flows (flow_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE INDEX IF NOT EXISTS counts_flow_id_idx
    ON gwolofs.counts USING btree
    (flow_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS counts_datetime_bin_idx
    ON gwolofs.counts USING btree
    (datetime_bin ASC NULLS LAST)
    TABLESPACE pg_default;

ALTER TABLE gwolofs.counts OWNER TO ecocounter_admins;

GRANT SELECT, INSERT, DELETE ON gwolofs.counts TO ecocounter_bot;

REVOKE ALL ON TABLE gwolofs.counts FROM bdit_humans;
GRANT SELECT ON gwolofs.counts TO bdit_humans;

COMMENT ON TABLE gwolofs.counts
IS 'Table contains the actual binned counts for ecocounter flows. Please note that bin size varies for older data, so averaging these numbers may not be straightforward.';

COMMENT ON COLUMN gwolofs.counts.datetime_bin
IS 'indicates start time of the time bin. Note that not all time bins are the same size!';