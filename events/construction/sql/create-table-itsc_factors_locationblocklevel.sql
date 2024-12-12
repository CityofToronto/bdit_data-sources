-- Table: itsc_factors.locationblocklevel

-- DROP TABLE IF EXISTS itsc_factors.locationblocklevel;

CREATE TABLE IF NOT EXISTS itsc_factors.locationblocklevel
(
    code integer NOT NULL,
    locationblocklevel text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT locationblocklevel_pkey PRIMARY KEY (code)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS itsc_factors.locationblocklevel
OWNER TO congestion_admins;

GRANT SELECT ON TABLE itsc_factors.locationblocklevel TO events_bot;