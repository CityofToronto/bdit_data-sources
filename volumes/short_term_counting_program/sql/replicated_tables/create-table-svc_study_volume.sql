-- Table: traffic.svc_study_volume

-- DROP TABLE IF EXISTS traffic.svc_study_volume;

CREATE TABLE IF NOT EXISTS traffic.svc_study_volume
(
    id bigint NOT NULL DEFAULT nextval('move_staging.svc_study_volume_id_seq'::regclass),
    study_id integer NOT NULL,
    count_info_id integer NOT NULL,
    direction text COLLATE pg_catalog."default" NOT NULL,
    count_date date NOT NULL,
    time_start timestamp without time zone NOT NULL,
    time_end timestamp without time zone NOT NULL,
    volume integer NOT NULL,
    CONSTRAINT svc_study_volume_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.svc_study_volume
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.svc_study_volume FROM bdit_humans;

GRANT SELECT ON TABLE traffic.svc_study_volume TO bdit_humans;

GRANT ALL ON TABLE traffic.svc_study_volume TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.svc_study_volume TO traffic_bot;

COMMENT ON TABLE traffic.svc_study_volume
IS 'Documentation: https://move-etladmin.intra.prod-toronto.ca/docs/database_schema.html#atr.table.study-volume-human.
Copied from "move_staging"."svc_study_volume" by bigdata repliactor DAG at 2025-07-04 13:50.';
-- Index: svc_study_volume_count_date_idx

-- DROP INDEX IF EXISTS traffic.svc_study_volume_count_date_idx;

CREATE INDEX IF NOT EXISTS svc_study_volume_count_date_idx
ON traffic.svc_study_volume USING btree
(count_date ASC NULLS LAST)
TABLESPACE pg_default;
