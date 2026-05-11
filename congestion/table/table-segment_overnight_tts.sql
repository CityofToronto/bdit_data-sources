-- Table: here_agg.segment_6month_lookback

-- DROP TABLE IF EXISTS here_agg.segment_6month_lookback;

CREATE TABLE IF NOT EXISTS here_agg.segment_6month_lookback
(
    segment_id integer NOT NULL,
    mnth date NOT NULL,
    overnight_avg_tt real,
    rolling_6month_quasi_obs integer,
    CONSTRAINT segment_6month_lookback_pkey PRIMARY KEY (segment_id, mnth),
    CONSTRAINT segment_6month_lookback_mnth_check CHECK (date_trunc('month'::text, mnth) = mnth)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.segment_6month_lookback
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.segment_6month_lookback FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.segment_6month_lookback TO bdit_humans;

GRANT ALL ON TABLE here_agg.segment_6month_lookback TO here_admins;

-- Index: segment_6month_lookback_dt_idx
-- DROP INDEX IF EXISTS here_agg.segment_6month_lookback_dt_idx;
CREATE INDEX IF NOT EXISTS segment_6month_lookback_dt_idx
ON here_agg.segment_6month_lookback USING btree
(mnth ASC NULLS LAST)
WITH (fillfactor = 100, deduplicate_items = TRUE)
TABLESPACE pg_default;

-- Index: segment_6month_lookback_segment_id_dt_idx
-- DROP INDEX IF EXISTS here_agg.segment_6month_lookback_segment_id_dt_idx;
CREATE INDEX IF NOT EXISTS segment_6month_lookback_segment_id_dt_idx
ON here_agg.segment_6month_lookback USING btree
(segment_id ASC NULLS LAST, mnth ASC NULLS LAST)
WITH (fillfactor = 100, deduplicate_items = TRUE)
TABLESPACE pg_default;

-- Index: segment_6month_lookback_segment_id_idx
-- DROP INDEX IF EXISTS here_agg.segment_6month_lookback_segment_id_idx;
CREATE INDEX IF NOT EXISTS segment_6month_lookback_segment_id_idx
ON here_agg.segment_6month_lookback USING btree
(segment_id ASC NULLS LAST)
WITH (fillfactor = 100, deduplicate_items = TRUE)
TABLESPACE pg_default;

COMMENT ON TABLE here_agg.segment_6month_lookback
IS 'Stores 6-month lookback segment overnight speed and vehicle km travelled (VKT) for TTI calculation.
`mnth` represents analysis month (data is from prior 6 months).';
