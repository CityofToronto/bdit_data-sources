-- Table: gwolofs.tt_segments

-- DROP TABLE IF EXISTS gwolofs.tt_segments;

CREATE TABLE IF NOT EXISTS gwolofs.tt_segments
(
    link_dirs text[] COLLATE pg_catalog."default",
    lengths numeric[],
    geom geometry,
    total_length numeric,
    uid smallint NOT NULL DEFAULT nextval('tt_segments_uid_seq'::regclass),
    CONSTRAINT unique_link_dirs UNIQUE (link_dirs)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.tt_segments OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.tt_segments FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.tt_segments TO bdit_humans;

GRANT ALL ON TABLE gwolofs.tt_segments TO gwolofs;