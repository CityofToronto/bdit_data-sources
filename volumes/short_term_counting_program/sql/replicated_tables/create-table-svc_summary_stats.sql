-- Table: traffic.svc_summary_stats

-- DROP TABLE IF EXISTS traffic.svc_summary_stats;

CREATE TABLE IF NOT EXISTS traffic.svc_summary_stats
(
    study_id integer NOT NULL,
    total_days numeric NOT NULL,
    total_veh numeric NOT NULL,
    daily_avg_vol numeric NOT NULL,
    total_weekend_days numeric,
    avg_weekend_vol numeric,
    total_weekdays numeric,
    avg_weekday_vol numeric,
    heavy_pct numeric,
    avg_weekday_am_peak_hour_start time without time zone,
    avg_weekday_am_peak_vol numeric,
    avg_weekday_pm_peak_hour_start time without time zone,
    avg_weekday_pm_peak_vol numeric,
    pct_15 numeric,
    pct_50 numeric,
    pct_85 numeric,
    pct_95 numeric,
    mean_speed numeric,
    CONSTRAINT summary_stats_pkey PRIMARY KEY (study_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.svc_summary_stats
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.svc_summary_stats FROM bdit_humans;

GRANT SELECT ON TABLE traffic.svc_summary_stats TO bdit_humans;

GRANT ALL ON TABLE traffic.svc_summary_stats TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.svc_summary_stats TO traffic_bot;

COMMENT ON TABLE traffic.svc_summary_stats
IS 'Documentation: https://move-etladmin.intra.prod-toronto.ca/docs/database_schema.html#atr.view.summary-stats.
Copied from "move_staging"."svc_summary_stats" by bigdata repliactor DAG at 2025-07-04 13:50.';
