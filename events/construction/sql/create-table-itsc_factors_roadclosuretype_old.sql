-- Table: itsc_factors.roadclosuretype_old

-- DROP TABLE IF EXISTS itsc_factors.roadclosuretype_old;

CREATE TABLE IF NOT EXISTS itsc_factors.roadclosuretype_old
(
    code integer NOT NULL,
    roadclosuretype text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT roadclosuretype_old_pkey PRIMARY KEY (code)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS itsc_factors.roadclosuretype_old
OWNER TO congestion_admins;