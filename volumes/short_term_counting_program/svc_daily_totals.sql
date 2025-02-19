CREATE OR REPLACE VIEW traffic.svc_daily_totals AS

WITH daily_totals AS (
    -- speed studies
    SELECT
        study_id,
        count_date,
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
        ) AS daily_volume
    FROM traffic.svc_study_speed
    GROUP BY
        study_id,
        count_date
    HAVING COUNT(*) = 2 * 4 * 24
    
    UNION
    
    -- volume studies
    SELECT
        study_id,
        count_date,
        SUM(volume) AS daily_volume
    FROM traffic.svc_study_volume
    GROUP BY
        study_id,
        count_date
    HAVING COUNT(*) = 2 * 4 * 24

    UNION

    -- classification studies
    SELECT
        study_id,
        count_date,
        SUM(
            -- check that these are mutually exclusive
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
        ) AS daily_volume
    FROM traffic.svc_study_class
    GROUP BY
        study_id,
        count_date
    HAVING COUNT(*) = 2 * 4 * 24
)

SELECT
    study_id,
    count_date,
    centreline_id,
    geom AS centreline_geom,
    daily_volume
FROM daily_totals
JOIN traffic.svc_metadata USING (study_id)
JOIN gis_core.centreline_latest USING (centreline_id);
