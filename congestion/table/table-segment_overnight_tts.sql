-- Table: here_agg.segment_overnight_tts

-- DROP TABLE IF EXISTS here_agg.segment_overnight_tts;

CREATE TABLE IF NOT EXISTS here_agg.segment_overnight_tts
(
    segment_id integer NOT NULL,
    mnth date NOT NULL,
    overnight_avg_tt real,
    rolling_6month_quasi_obs integer,
    CONSTRAINT segment_overnight_tts_pkey PRIMARY KEY (segment_id, mnth)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.segment_overnight_tts
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.segment_overnight_tts FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.segment_overnight_tts TO bdit_humans;

GRANT ALL ON TABLE here_agg.segment_overnight_tts TO here_admins;

GRANT ALL ON TABLE here_agg.segment_overnight_tts TO rds_superuser WITH GRANT OPTION;

-- Index: segment_overnight_tts_dt_idx

-- DROP INDEX IF EXISTS here_agg.segment_overnight_tts_dt_idx;

CREATE INDEX IF NOT EXISTS segment_overnight_tts_dt_idx
ON here_agg.segment_overnight_tts USING btree
(mnth ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True)
TABLESPACE pg_default;

-- Index: segment_overnight_tts_segment_id_dt_idx

-- DROP INDEX IF EXISTS here_agg.segment_overnight_tts_segment_id_dt_idx;

CREATE INDEX IF NOT EXISTS segment_overnight_tts_segment_id_dt_idx
ON here_agg.segment_overnight_tts USING btree
(segment_id ASC NULLS LAST, mnth ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True)
TABLESPACE pg_default;

-- Index: segment_overnight_tts_segment_id_dt_idx

-- DROP INDEX IF EXISTS here_agg.segment_overnight_tts_segment_id_dt_idx;

CREATE INDEX IF NOT EXISTS segment_overnight_tts_segment_id_dt_idx
ON here_agg.segment_overnight_tts USING btree
(segment_id ASC NULLS LAST, mnth ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True)
TABLESPACE pg_default;

-- Index: segment_overnight_tts_segment_id_idx

-- DROP INDEX IF EXISTS here_agg.segment_overnight_tts_segment_id_idx;

CREATE INDEX IF NOT EXISTS segment_overnight_tts_segment_id_idx
ON here_agg.segment_overnight_tts USING btree
(segment_id ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True)
TABLESPACE pg_default;
