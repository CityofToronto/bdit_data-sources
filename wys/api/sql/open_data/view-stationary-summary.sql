-- View: open_data.wys_stationary_summary

DROP VIEW open_data.wys_stationary_summary;

CREATE OR REPLACE VIEW open_data.wys_stationary_summary AS
SELECT
    od.sign_id,
    agg.mon AS "month",
    agg.pct_05,
    agg.pct_10,
    agg.pct_15,
    agg.pct_20,
    agg.pct_25,
    agg.pct_30,
    agg.pct_35,
    agg.pct_40,
    agg.pct_45,
    agg.pct_50,
    agg.pct_55,
    agg.pct_60,
    agg.pct_65,
    agg.pct_70,
    agg.pct_75,
    agg.pct_80,
    agg.pct_85,
    agg.pct_90,
    agg.pct_95,
    agg.spd_00,
    agg.spd_05,
    agg.spd_10,
    agg.spd_15,
    agg.spd_20,
    agg.spd_25,
    agg.spd_30,
    agg.spd_35,
    agg.spd_40,
    agg.spd_45,
    agg.spd_50,
    agg.spd_55,
    agg.spd_60,
    agg.spd_65,
    agg.spd_70,
    agg.spd_75,
    agg.spd_80,
    agg.spd_85,
    agg.spd_90,
    agg.spd_95,
    agg.spd_100_and_above,
    agg.volume
FROM open_data.wys_stationary_locations AS od
JOIN wys.stationary_signs AS loc USING (sign_id)
JOIN wys.stationary_summary AS agg ON
    loc.sign_id = agg.sign_id 
    AND agg.mon >= od.start_date 
    AND (
        od.end_date IS NULL
        OR agg.mon < od.end_date
    );

ALTER TABLE open_data.wys_stationary_summary OWNER TO wys_admins;

GRANT SELECT ON TABLE open_data.wys_stationary_summary TO od_extract_svc;
GRANT SELECT ON TABLE open_data.wys_stationary_summary TO bdit_humans;
