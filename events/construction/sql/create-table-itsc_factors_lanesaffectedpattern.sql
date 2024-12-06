-- Table: itsc_factors.lanesaffectedpattern

-- DROP TABLE IF EXISTS itsc_factors.lanesaffectedpattern;

CREATE TABLE IF NOT EXISTS itsc_factors.lanesaffectedpattern
(
    lane_status text COLLATE pg_catalog."default" NOT NULL,
    code text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT lanesaffected_pkey PRIMARY KEY (code)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS itsc_factors.lanesaffectedpattern
OWNER TO congestion_admins;

REVOKE ALL ON TABLE itsc_factors.lanesaffectedpattern FROM bdit_humans;

GRANT INSERT, REFERENCES, SELECT, TRIGGER, UPDATE ON TABLE itsc_factors.lanesaffectedpattern TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE itsc_factors.lanesaffectedpattern TO congestion_admins;

GRANT ALL ON TABLE itsc_factors.lanesaffectedpattern TO rds_superuser WITH GRANT OPTION;