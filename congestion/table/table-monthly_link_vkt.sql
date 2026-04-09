-- Table: here_agg.monthly_link_vkt

-- DROP TABLE IF EXISTS here_agg.monthly_link_vkt;

CREATE TABLE IF NOT EXISTS here_agg.monthly_link_vkt
(
    mnth timestamp without time zone NOT NULL,
    link_dir text COLLATE pg_catalog."default" NOT NULL,
    length double precision,
    sample_size bigint,
    ver_id text COLLATE pg_catalog."default",
    CONSTRAINT mnth_link_pkey PRIMARY KEY (mnth, link_dir)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.monthly_link_vkt
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.monthly_link_vkt FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.monthly_link_vkt TO bdit_humans;
