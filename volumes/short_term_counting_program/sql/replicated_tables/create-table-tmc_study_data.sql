-- Table: traffic.tmc_study_data

-- DROP TABLE IF EXISTS traffic.tmc_study_data;

CREATE TABLE IF NOT EXISTS traffic.tmc_study_data
(
    id bigint NOT NULL DEFAULT nextval('move_staging.tmc_study_data_id_seq'::regclass),
    count_id bigint NOT NULL,
    time_start timestamp without time zone NOT NULL,
    time_end timestamp without time zone NOT NULL,
    n_cars_r integer NOT NULL,
    n_cars_t integer NOT NULL,
    n_cars_l integer NOT NULL,
    s_cars_r integer NOT NULL,
    s_cars_t integer NOT NULL,
    s_cars_l integer NOT NULL,
    e_cars_r integer NOT NULL,
    e_cars_t integer NOT NULL,
    e_cars_l integer NOT NULL,
    w_cars_r integer NOT NULL,
    w_cars_t integer NOT NULL,
    w_cars_l integer NOT NULL,
    n_truck_r integer NOT NULL,
    n_truck_t integer NOT NULL,
    n_truck_l integer NOT NULL,
    s_truck_r integer NOT NULL,
    s_truck_t integer NOT NULL,
    s_truck_l integer NOT NULL,
    e_truck_r integer NOT NULL,
    e_truck_t integer NOT NULL,
    e_truck_l integer NOT NULL,
    w_truck_r integer NOT NULL,
    w_truck_t integer NOT NULL,
    w_truck_l integer NOT NULL,
    n_bus_r integer NOT NULL,
    n_bus_t integer NOT NULL,
    n_bus_l integer NOT NULL,
    s_bus_r integer NOT NULL,
    s_bus_t integer NOT NULL,
    s_bus_l integer NOT NULL,
    e_bus_r integer NOT NULL,
    e_bus_t integer NOT NULL,
    e_bus_l integer NOT NULL,
    w_bus_r integer NOT NULL,
    w_bus_t integer NOT NULL,
    w_bus_l integer NOT NULL,
    n_peds integer NOT NULL,
    s_peds integer NOT NULL,
    e_peds integer NOT NULL,
    w_peds integer NOT NULL,
    n_bike integer NOT NULL,
    s_bike integer NOT NULL,
    e_bike integer NOT NULL,
    w_bike integer NOT NULL,
    n_other integer NOT NULL,
    s_other integer NOT NULL,
    e_other integer NOT NULL,
    w_other integer NOT NULL,
    CONSTRAINT tmc_study_data_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.tmc_study_data
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.tmc_study_data FROM bdit_humans;

GRANT SELECT ON TABLE traffic.tmc_study_data TO bdit_humans;

GRANT ALL ON TABLE traffic.tmc_study_data TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.tmc_study_data TO traffic_bot;

COMMENT ON TABLE traffic.tmc_study_data
IS 'Documentation: https://move-etladmin.intra.prod-toronto.ca/docs/database_schema.html#tmc.table.study_human.
Copied from "move_staging"."tmc_study_data" by bigdata repliactor DAG at 2025-07-04 13:50.';
-- Index: tmc_study_data_time_start_idx

-- DROP INDEX IF EXISTS traffic.tmc_study_data_time_start_idx;

CREATE INDEX IF NOT EXISTS tmc_study_data_time_start_idx
ON traffic.tmc_study_data USING btree
(time_start ASC NULLS LAST)
TABLESPACE pg_default;
