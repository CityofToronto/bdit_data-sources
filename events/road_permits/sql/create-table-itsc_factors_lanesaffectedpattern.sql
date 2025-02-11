-- Table: itsc_factors.lanesaffectedpattern

-- DROP TABLE IF EXISTS itsc_factors.lanesaffectedpattern;

CREATE TABLE IF NOT EXISTS itsc_factors.lanesaffectedpattern
(
    lane_status text COLLATE pg_catalog."default",
    code text COLLATE pg_catalog."default" NOT NULL,
    mode text COLLATE pg_catalog."default",
    lane_open numeric,
    lane_closed numeric,
    CONSTRAINT lanesaffected_pattern_pkey PRIMARY KEY (code)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS itsc_factors.lanesaffectedpattern
OWNER TO congestion_admins;

REVOKE ALL ON TABLE itsc_factors.lanesaffectedpattern FROM bdit_humans;

GRANT SELECT ON TABLE itsc_factors.lanesaffectedpattern TO bdit_humans;

GRANT SELECT ON TABLE itsc_factors.lanesaffectedpattern TO events_bot;
