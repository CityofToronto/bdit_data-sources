-- Table: gwolofs.tt_segments

-- DROP TABLE IF EXISTS gwolofs.tt_segments;

CREATE TABLE IF NOT EXISTS gwolofs.tt_segments
(
    link_dirs text[] COLLATE pg_catalog."default",
    lengths numeric[],
    geom geometry,
    total_length numeric,
    uid smallint NOT NULL DEFAULT nextval('tt_segments_uid_seq'::regclass),
    node_start bigint NOT NULL,
    node_end bigint NOT NULL,
    map_version text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT tt_segments_pkey PRIMARY KEY (node_start, node_end, map_version)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.tt_segments
    OWNER to gwolofs;

REVOKE ALL ON TABLE gwolofs.tt_segments FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.tt_segments TO bdit_humans;

GRANT ALL ON TABLE gwolofs.tt_segments TO gwolofs;