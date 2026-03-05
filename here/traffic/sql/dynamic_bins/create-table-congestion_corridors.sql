-- Table: here_agg.corridors

-- DROP TABLE IF EXISTS here_agg.corridors;

CREATE TABLE IF NOT EXISTS here_agg.corridors
(
    link_dirs text [] COLLATE pg_catalog."default",
    lengths numeric [],
    geom geometry,
    total_length numeric,
    corridor_id smallint NOT NULL DEFAULT nextval('congestion_corridors_uid_seq'::regclass),
    node_start bigint NOT NULL,
    node_end bigint NOT NULL,
    map_version text COLLATE pg_catalog."default" NOT NULL,
    corridor_streets text COLLATE pg_catalog."default",
    corridor_start text COLLATE pg_catalog."default",
    corridor_end text COLLATE pg_catalog."default",
    project_id integer,
    CONSTRAINT congestion_corridors_pkey PRIMARY KEY (node_start, node_end, map_version),
    CONSTRAINT corridor_pkey UNIQUE NULLS NOT DISTINCT (corridor_id),
    CONSTRAINT project_id_fk FOREIGN KEY (project_id)
    REFERENCES here_agg.projects (project_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.corridors
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.corridors FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.corridors TO bdit_humans;

COMMENT ON TABLE here_agg.corridors IS
'Stores cached travel time corridors to reduce routing time.';
