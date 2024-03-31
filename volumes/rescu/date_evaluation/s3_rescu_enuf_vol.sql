CREATE TABLE scannon.rescu_enuf_vol_1 AS (
    WITH vol_asmt AS (
        SELECT 
            dv.*,
            di.number_of_lanes,
            CASE
                WHEN di.primary_road LIKE 'RAMP%' THEN 'ramp'
                WHEN di.det_group = 'ALLEN' AND dv.day_type = 'Weekday' AND dv.d_vol / di.number_of_lanes  >= 4000 THEN 'enough volume'
                WHEN di.det_group = 'ALLEN' AND dv.day_type = 'Weekend' AND dv.d_vol / di.number_of_lanes  >= 3000 THEN 'enough volume'
                WHEN di.det_group = 'DVP' AND dv.day_type = 'Weekday' AND dv.d_vol / di.number_of_lanes  >= 15000 THEN 'enough volume'
                WHEN di.det_group = 'DVP' AND dv.day_type = 'Weekend' AND dv.d_vol / di.number_of_lanes  >= 10000 THEN 'enough volume'
                WHEN di.det_group = 'FGG' AND dv.d_vol / di.number_of_lanes  >= 10000 THEN 'enough volume'
                WHEN di.det_group = 'LAKE' AND dv.d_vol / di.number_of_lanes  >= 2000 THEN 'enough volume'
                ELSE 'check volume'
            END AS vol_check
        FROM scannon.rescu_dayvol_stats_21 AS dv
        LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
    )

    SELECT 
        va.* 
    FROM vol_asmt AS va
    WHERE va.vol_check = 'enough volume'
);
    
COMMENT ON TABLE scannon.rescu_enuf_vol IS 'enough volume for the Gardiner, DVP and Allen is calculated per lane using the thresholds above. Ramps are excluded (for now)';