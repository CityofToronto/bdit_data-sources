-- Table: here_agg.projects

-- DROP TABLE IF EXISTS here_agg.projects;

CREATE TABLE IF NOT EXISTS here_agg.projects
(
    project_id integer NOT NULL DEFAULT nextval('congestion_projects_project_id_seq'::regclass),
    description text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT congestion_projects_pkey PRIMARY KEY (project_id),
    CONSTRAINT unique_prj_description UNIQUE NULLS NOT DISTINCT (description)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.projects
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.projects FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.projects TO bdit_humans;
