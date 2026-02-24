-- Table: here.ta_path_hm

-- DROP TABLE IF EXISTS here.ta_path_hm;

CREATE TABLE IF NOT EXISTS here.ta_path_hm
(
    link_dir text COLLATE pg_catalog."default" NOT NULL,
    tx timestamp NOT NULL,
    mean numeric(4,1) NOT NULL,
    harmonic_mean numeric(4,1),
    stddev numeric(4,1) NOT NULL,
    min_spd integer NOT NULL,
    max_spd integer NOT NULL,
    pct_50 integer NOT NULL,
    pct_85 integer NOT NULL,
    sample_size integer
) PARTITION BY RANGE (tx);

ALTER TABLE IF EXISTS here.ta_path_hm OWNER to here_admins;

REVOKE ALL ON TABLE here.ta_path_hm FROM bdit_humans;
REVOKE ALL ON TABLE here.ta_path_hm FROM congestion_bot;
REVOKE ALL ON TABLE here.ta_path_hm FROM covid_admins;
REVOKE ALL ON TABLE here.ta_path_hm FROM here_bot;
REVOKE ALL ON TABLE here.ta_path_hm FROM tt_request_bot;

GRANT SELECT ON TABLE here.ta_path_hm TO bdit_humans;
GRANT SELECT ON TABLE here.ta_path_hm TO congestion_bot;
GRANT SELECT ON TABLE here.ta_path_hm TO covid_admins;
GRANT ALL ON TABLE here.ta_path_hm TO here_admins;
GRANT SELECT ON TABLE here.ta_path_hm TO here_bot;
GRANT SELECT ON TABLE here.ta_path_hm TO tt_request_bot;

COMMENT ON TABLE here.ta_path_hm
IS '(In development) HERE Path data pulled from Traffic Analytics API. Includes harmonic mean.';

CREATE INDEX ta_path_hm_dt_idx
ON here.ta_path_hm USING btree
(dt ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True);
-- Index: ta_path_hm_link_dir_idx

-- DROP INDEX IF EXISTS here.ta_path_hm_link_dir_idx;

CREATE INDEX ta_path_hm_link_dir_idx
ON here.ta_path_hm USING btree
(link_dir COLLATE pg_catalog."default" ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True);
-- Index: ta_path_hm_tod_idx

-- DROP INDEX IF EXISTS here.ta_path_hm_tod_idx;

CREATE INDEX ta_path_hm_tod_idx
ON here.ta_path_hm USING btree
(tod ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True);

ALTER TABLE here.ta_path_hm ADD
CONSTRAINT ta_path_hm_dt_tod_link_dir_key UNIQUE (dt, tod, link_dir)

--create partitions
SELECT here.create_weekly_partitions(2024, 'here', 'ta_path_hm');
SELECT here.create_weekly_partitions(2025, 'here', 'ta_path_hm');
