-- Table: traffic.svc_study_speed

-- DROP TABLE IF EXISTS traffic.svc_study_speed;

CREATE TABLE IF NOT EXISTS traffic.svc_study_speed
(
    id bigint NOT NULL DEFAULT nextval('move_staging.svc_study_speed_id_seq'::regclass),
    study_id integer NOT NULL,
    count_info_id integer NOT NULL,
    direction text COLLATE pg_catalog."default" NOT NULL,
    count_date date NOT NULL,
    time_start timestamp without time zone NOT NULL,
    time_end timestamp without time zone NOT NULL,
    vol_1_19_kph integer NOT NULL,
    vol_20_25_kph integer NOT NULL,
    vol_26_30_kph integer NOT NULL,
    vol_31_35_kph integer NOT NULL,
    vol_36_40_kph integer NOT NULL,
    vol_41_45_kph integer NOT NULL,
    vol_46_50_kph integer NOT NULL,
    vol_51_55_kph integer NOT NULL,
    vol_56_60_kph integer NOT NULL,
    vol_61_65_kph integer NOT NULL,
    vol_66_70_kph integer NOT NULL,
    vol_71_75_kph integer NOT NULL,
    vol_76_80_kph integer NOT NULL,
    vol_81_160_kph integer NOT NULL,
    CONSTRAINT svc_study_speed_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.svc_study_speed
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.svc_study_speed FROM bdit_humans;

GRANT SELECT ON TABLE traffic.svc_study_speed TO bdit_humans;

GRANT ALL ON TABLE traffic.svc_study_speed TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.svc_study_speed TO traffic_bot;

COMMENT ON TABLE traffic.svc_study_speed
IS 'Documentation: https://move-etladmin.intra.prod-toronto.ca/docs/database_schema.html#atr.table.study-speed-human.
Copied from "move_staging"."svc_study_speed" by bigdata repliactor DAG at 2025-07-04 13:50.';
-- Index: svc_study_speed_count_date_idx

-- DROP INDEX IF EXISTS traffic.svc_study_speed_count_date_idx;

CREATE INDEX IF NOT EXISTS svc_study_speed_count_date_idx
ON traffic.svc_study_speed USING btree
((time_start::date) ASC NULLS LAST)
TABLESPACE pg_default;
