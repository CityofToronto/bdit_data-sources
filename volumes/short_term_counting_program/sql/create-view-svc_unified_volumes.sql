CREATE OR REPLACE VIEW traffic.svc_unified_volumes AS

-- speed studies
SELECT
    study_id,
    count_date,
    time_start,
    time_end,
    direction,
    SUM(
        vol_1_19_kph
        + vol_20_25_kph
        + vol_26_30_kph
        + vol_31_35_kph
        + vol_36_40_kph
        + vol_41_45_kph
        + vol_46_50_kph
        + vol_51_55_kph
        + vol_56_60_kph
        + vol_61_65_kph
        + vol_66_70_kph
        + vol_71_75_kph
        + vol_76_80_kph
        + vol_81_160_kph
    ) AS volume
FROM traffic.svc_study_speed
GROUP BY
    study_id,
    count_date,
    time_start,
    time_end,
    direction

UNION

-- volume studies
SELECT
    study_id,
    count_date,
    time_start,
    time_end,
    direction,
    SUM(volume) AS volume
FROM traffic.svc_study_volume
GROUP BY
    study_id,
    count_date,
    time_start,
    time_end,
    direction

UNION

-- classification studies
SELECT
    study_id,
    count_date,
    time_start,
    time_end,
    direction,
    SUM(
        motorcycle
        + cars
        + "2a_4t"
        + buses
        + "2a_su"
        + "3a_su"
        + "4a_su"
        + "4a_st"
        + "5a_st"
        + "6a_st"
        + "5a_mt"
        + "6a_mt"
    ) AS volume
FROM traffic.svc_study_class
GROUP BY
    study_id,
    count_date,
    time_start,
    time_end,
    direction;

ALTER VIEW traffic.svc_unified_volumes OWNER TO traffic_admins;

GRANT SELECT ON TABLE traffic.svc_unified_volumes TO bdit_humans;

COMMENT ON VIEW traffic.svc_unified_volumes IS
'A unified view of Speed, Volume, and Classification study volumes by 15 minute bin.';
