CREATE TABLE scannon.rescu_enuf_vol AS (
    WITH vol_asmt AS (
        SELECT 
            dv.*,
            di.number_of_lanes,
            CASE
                WHEN di.primary_road LIKE 'RAMP%' THEN 'ramp'
                WHEN di.det_group IN ('FGG', 'ALLEN', 'DVP') AND dv.d_vol >= 20000 THEN 'enough volume'
                WHEN di.det_group IN ('LAKE') AND dv.d_vol >= 10000 THEN 'enough volume'
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
    
COMMENT ON TABLE scannon.rescu_enuf_vol IS 'enough volume for the Gardiner, DVP and Allen is 20000 per day; for Lakeshore the figure is 10000 per day.
Ramps are excluded (for now)';