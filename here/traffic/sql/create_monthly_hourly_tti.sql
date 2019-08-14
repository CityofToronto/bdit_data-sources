-- Table: here_eval.monthly_avg

-- DROP TABLE here_eval.monthly_avg;

CREATE TABLE here.monthly_hourly_tti
(
    link_dir text COLLATE pg_catalog."default" NOT NULL,
    mnth date NOT NULL,
    hh text COLLATE pg_catalog."default" NOT NULL,
    tt_avg numeric NOT NULL,
    tt_med numeric NOT NULL,
    tti_avg numeric NOT NULL,
	obs integer NOT NULL,
	UNIQUE(link_dir, mnth, hh)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE here_eval.monthly_avg
    OWNER to rdumas;

GRANT SELECT, REFERENCES ON TABLE here_eval.monthly_avg TO bdit_humans;

GRANT ALL ON TABLE here_eval.monthly_avg TO rdumas;
