-- Table: here_agg.hourly_avg_tt

-- DROP TABLE IF EXISTS here_agg.hourly_avg_tt;

CREATE TABLE IF NOT EXISTS here_agg.hourly_avg_tt
(
    segment_id integer NOT NULL,
    dt date NOT NULL,
    hr smallint NOT NULL,
    avg_tt double precision,
    CONSTRAINT hourly_avg_tt_pkey PRIMARY KEY (segment_id, dt, hr)
);

ALTER TABLE IF EXISTS here_agg.hourly_avg_tt OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.hourly_avg_tt FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.hourly_avg_tt TO bdit_humans;

GRANT ALL ON TABLE here_agg.hourly_avg_tt TO here_admins;

CREATE INDEX IF NOT EXISTS hourly_avg_tt_segment_id_dt_idx
ON here_agg.hourly_avg_tt USING btree
(segment_id ASC NULLS LAST, dt ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True)
TABLESPACE pg_default;
