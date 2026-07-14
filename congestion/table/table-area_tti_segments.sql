-- Table: here_agg.area_tti_segments

-- DROP TABLE IF EXISTS here_agg.area_tti_segments;

CREATE TABLE IF NOT EXISTS here_agg.area_tti_segments
(
    area_name character varying COLLATE pg_catalog."default" NOT NULL,
    segment_id integer NOT NULL,
    highway boolean NOT NULL,
    dt date NOT NULL,
    hr integer NOT NULL,
    tti double precision,
    pkt_km double precision,
    weighted_tti double precision,
    CONSTRAINT area_tti_segments_pkey PRIMARY KEY (area_name, segment_id, highway, dt, hr)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.area_tti_segments
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.area_tti_segments FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.area_tti_segments TO bdit_humans;

GRANT ALL ON TABLE here_agg.area_tti_segments TO here_admins;

COMMENT ON TABLE here_agg.area_tti_segments
IS 'Stores detailed segment level results for here_agg.area_tti.';
