-- Table: traffic.tmc_summary_stats

-- DROP TABLE IF EXISTS traffic.tmc_summary_stats;

CREATE TABLE IF NOT EXISTS traffic.tmc_summary_stats
(
    count_id bigint NOT NULL,
    count_veh_total numeric NOT NULL,
    count_heavy_pct_total numeric NOT NULL,
    count_total_bikes numeric NOT NULL,
    count_total_peds numeric NOT NULL,
    am_peak_time_start timestamp without time zone,
    am_peak_total_veh numeric,
    am_peak_heavy_pct numeric,
    am_peak_bikes numeric,
    pm_peak_time_start timestamp without time zone,
    pm_peak_total_veh numeric,
    pm_peak_heavy_pct numeric,
    pm_peak_bikes numeric,
    count_veh_n_appr numeric NOT NULL,
    count_heavy_pct_n_appr numeric NOT NULL,
    count_bikes_n_appr numeric NOT NULL,
    count_veh_e_appr numeric NOT NULL,
    count_heavy_pct_e_appr numeric NOT NULL,
    count_bikes_e_appr numeric NOT NULL,
    count_veh_s_appr numeric NOT NULL,
    count_heavy_pct_s_appr numeric NOT NULL,
    count_bikes_s_appr numeric NOT NULL,
    count_veh_w_appr numeric NOT NULL,
    count_heavy_pct_w_appr numeric NOT NULL,
    count_bikes_w_appr numeric NOT NULL,
    CONSTRAINT tmc_summary_stats_pkey PRIMARY KEY (count_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.tmc_summary_stats
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.tmc_summary_stats FROM bdit_humans;

GRANT SELECT ON TABLE traffic.tmc_summary_stats TO bdit_humans;

GRANT ALL ON TABLE traffic.tmc_summary_stats TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.tmc_summary_stats TO traffic_bot;

COMMENT ON TABLE traffic.tmc_summary_stats
IS 'Count level summary statistics for all TMCs.
Documentation: https://github.com/CityofToronto/bdit_data-sources/blob/master/volumes/short_term_counting_program/README.md#traffictmc_summary_stats';
