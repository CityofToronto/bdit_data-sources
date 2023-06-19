-- Populate centreline_ids for rescu detector stations that were missing this information

CREATE TABLE scannon.rescu_data_21 AS (
    WITH cent_blanks AS (
        SELECT
            gv.detector_id,
            gv.arterycode,
            mc.centreline_id,
            gv.gen_loc,
            gv.direction,
            gv.datetime_bin,
            gv.volume_15min
        FROM scannon.rescu_good_vol_21 AS gv
        LEFT JOIN scannon.rescu_miss_cid AS mc USING (detector_id)
        WHERE gv.centreline_id IS NULL
    )

    SELECT cb.*
    FROM cent_blanks AS cb
    
    UNION
    
    SELECT gv.*
    FROM scannon.rescu_good_vol_21 AS gv
    WHERE gv.centreline_id IS NOT NULL
);