-- Table: here_agg.monthly_segment_vkt

-- DROP TABLE IF EXISTS here_agg.monthly_segment_vkt;

CREATE TABLE IF NOT EXISTS here_agg.monthly_segment_vkt
(
    mnth timestamp without time zone NOT NULL,
    segment_id bigint NOT NULL,
    ver_id text NOT NULL,
    highway boolean,
    sample_size numeric,
    vkt_km double precision,
    sqrt_vkt_km double precision,
    CONSTRAINT monthly_segment_vkt_pkey PRIMARY KEY (mnth, segment_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.monthly_segment_vkt
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.monthly_segment_vkt FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.monthly_segment_vkt TO bdit_humans;
