-- Table: itsc_factors.direction

-- DROP TABLE IF EXISTS itsc_factors.direction;

CREATE TABLE IF NOT EXISTS itsc_factors.direction
(
    code integer NOT NULL,
    direction text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT itsc_factors_direction_pkey PRIMARY KEY (code)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS itsc_factors.direction
OWNER TO congestion_admins;

GRANT SELECT ON TABLE itsc_factors.direction TO events_bot;