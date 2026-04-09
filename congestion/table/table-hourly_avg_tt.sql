-- Table: here_agg.hourly_avg_tt

-- DROP TABLE IF EXISTS here_agg.hourly_avg_tt;

CREATE TABLE IF NOT EXISTS here_agg.hourly_avg_tt
(
    segment_id integer NOT NULL,
    dt date NOT NULL,
    hr smallint NOT NULL,
    is_wkdy boolean NOT NULL,
    avg_tt double precision,
    CONSTRAINT hourly_avg_tt_pkey PRIMARY KEY (segment_id, dt, hr, is_wkdy)
);

ALTER TABLE IF EXISTS here_agg.hourly_avg_tt OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.hourly_avg_tt FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.hourly_avg_tt TO bdit_humans;

GRANT ALL ON TABLE here_agg.hourly_avg_tt TO here_admins;
