-- Table: here_agg.area_tti

-- DROP TABLE IF EXISTS here_agg.area_tti;

CREATE TABLE IF NOT EXISTS here_agg.area_tti
(
    area_name text COLLATE pg_catalog."default" NOT NULL,
    dt date NOT NULL,
    hr smallint NOT NULL,
    tti double precision,
    num_segments integer,
    road_category text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT area_tti_pkey PRIMARY KEY (area_name, dt, hr, road_category)
);

ALTER TABLE IF EXISTS here_agg.area_tti OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.area_tti FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.area_tti TO bdit_humans;

GRANT ALL ON TABLE here_agg.area_tti TO here_admins;
