-- Table: gwolofs.congestion_corridors

-- DROP TABLE IF EXISTS gwolofs.congestion_corridors;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_corridors
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
    REFERENCES gwolofs.congestion_projects (project_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_corridors
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_corridors FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.congestion_corridors TO bdit_humans;

GRANT ALL ON TABLE gwolofs.congestion_corridors TO gwolofs;

COMMENT ON TABLE gwolofs.congestion_corridors IS
'Stores cached travel time corridors to reduce routing time.';
