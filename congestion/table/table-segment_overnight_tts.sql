-- Table: here_agg.segment_overnight_tts

-- DROP TABLE IF EXISTS here_agg.segment_overnight_tts;

CREATE TABLE IF NOT EXISTS here_agg.segment_overnight_tts
(
    segment_id integer NOT NULL,
    mnth date NOT NULL,
    is_wkdy boolean NOT NULL,
    overnight_avg_tt real,
    rolling_6month_quasi_obs integer,
    CONSTRAINT segment_overnight_tts_pkey PRIMARY KEY (segment_id, mnth, is_wkdy)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.segment_overnight_tts
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.segment_overnight_tts FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.segment_overnight_tts TO bdit_humans;
