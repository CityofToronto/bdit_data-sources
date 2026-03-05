-- Table: here_agg.projects

-- DROP TABLE IF EXISTS here_agg.projects;

CREATE TABLE IF NOT EXISTS here_agg.projects
(
    project_id integer NOT NULL DEFAULT nextval('here_agg.congestion_projects_project_id_seq'::regclass),
    description text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT congestion_projects_pkey PRIMARY KEY (project_id),
    CONSTRAINT unique_prj_description UNIQUE NULLS NOT DISTINCT (description)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.projects
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.projects FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.projects TO bdit_humans;

-- SEQUENCE: here_agg.congestion_projects_project_id_seq

-- DROP SEQUENCE IF EXISTS here_agg.congestion_projects_project_id_seq;

CREATE SEQUENCE IF NOT EXISTS here_agg.congestion_projects_project_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

ALTER SEQUENCE here_agg.congestion_projects_project_id_seq
    OWNED BY here_agg.congestion_projects.project_id;

ALTER SEQUENCE here_agg.congestion_projects_project_id_seq
    OWNER TO here_admins;
