-- Table: gwolofs.congestion_projects

-- DROP TABLE IF EXISTS gwolofs.congestion_projects;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_projects
(
    project_id integer NOT NULL DEFAULT nextval('congestion_projects_project_id_seq'::regclass),
    description text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT congestion_projects_pkey PRIMARY KEY (project_id),
    CONSTRAINT unique_prj_description UNIQUE NULLS NOT DISTINCT (description)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_projects
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_projects FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.congestion_projects TO bdit_humans;

GRANT ALL ON TABLE gwolofs.congestion_projects TO gwolofs;
