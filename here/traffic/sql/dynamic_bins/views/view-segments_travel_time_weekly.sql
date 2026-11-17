-- View: here_agg.segment_travel_times_weekly_view

-- DROP VIEW here_agg.segment_travel_times_weekly_view;

CREATE OR REPLACE VIEW here_agg.segment_travel_times_weekly_view AS
SELECT
    sbm.segment_id,
    cc.streetname,
    (cc.from_int_desc || ' to '::text) || cc.to_int_desc AS from_to_desc,
    cs.dir,
    sbm.length,
    sbm.dow_group,
    sbm.week_start,
    sbm.holiday_exceptions,
    sbm.hr_start,
    sbm.hr_end,
    round(sbm.avg_tt::numeric, 2) AS avg_tt,
    round(sbm.avg_ci_lower::numeric, 2) AS avg_ci_lower,
    round(sbm.avg_ci_upper::numeric, 2) AS avg_ci_upper,
    round(sbm.q1_tt::numeric, 2) AS q1_tt,
    round(sbm.q1_ci_lower::numeric, 2) AS q1_ci_lower,
    round(sbm.q1_ci_upper::numeric, 2) AS q1_ci_upper,
    round(sbm.median_tt::numeric, 2) AS median_tt,
    round(sbm.median_ci_lower::numeric, 2) AS median_ci_lower,
    round(sbm.median_ci_upper::numeric, 2) AS median_ci_upper,
    round(sbm.q3_tt::numeric, 2) AS q3_tt,
    round(sbm.q3_ci_lower::numeric, 2) AS q3_ci_lower,
    round(sbm.q3_ci_upper::numeric, 2) AS q3_ci_upper,
    sbm.n,
    round(sbm.tti::numeric, 2) AS tti
FROM here_agg.segment_travel_times_weekly sbm
JOIN congestion.congestion_centreline cc USING (segment_id, ver_id)
JOIN congestion.congestion_segments cs USING (segment_id, ver_id);

ALTER TABLE here_agg.segment_travel_times_weekly_view
OWNER TO here_admins;

GRANT SELECT ON TABLE here_agg.segment_travel_times_weekly_view TO bdit_humans;
GRANT ALL ON TABLE here_agg.segment_travel_times_weekly_view TO here_admins;
