-- Table: traffic.svc_study_class

-- DROP TABLE IF EXISTS traffic.svc_study_class;

CREATE TABLE IF NOT EXISTS traffic.svc_study_class
(
    id bigint NOT NULL DEFAULT nextval('move_staging.svc_study_class_id_seq'::regclass),
    study_id integer NOT NULL,
    count_info_id integer NOT NULL,
    direction text COLLATE pg_catalog."default" NOT NULL,
    count_date date NOT NULL,
    time_start timestamp without time zone NOT NULL,
    time_end timestamp without time zone NOT NULL,
    motorcycle integer NOT NULL,
    cars integer NOT NULL,
    "2a_4t" integer NOT NULL,
    buses integer NOT NULL,
    "2a_su" integer NOT NULL,
    "3a_su" integer NOT NULL,
    "4a_su" integer NOT NULL,
    "4a_st" integer NOT NULL,
    "5a_st" integer NOT NULL,
    "6a_st" integer NOT NULL,
    "5a_mt" integer NOT NULL,
    "6a_mt" integer NOT NULL,
    other integer NOT NULL,
    CONSTRAINT study_class_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.svc_study_class
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.svc_study_class FROM bdit_humans;

GRANT SELECT ON TABLE traffic.svc_study_class TO bdit_humans;

GRANT ALL ON TABLE traffic.svc_study_class TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.svc_study_class TO traffic_bot;

COMMENT ON TABLE traffic.svc_study_class
IS 'Documentation: https://move-etladmin.intra.prod-toronto.ca/docs/database_schema.html#atr.table.study-class-human.
Copied from "move_staging"."svc_study_class" by bigdata repliactor DAG at 2025-07-04 13:50.';
-- Index: svc_study_class_count_date_idx

-- DROP INDEX IF EXISTS traffic.svc_study_class_count_date_idx;

CREATE INDEX IF NOT EXISTS svc_study_class_count_date_idx
ON traffic.svc_study_class USING btree
(count_date ASC NULLS LAST)
TABLESPACE pg_default;
