CREATE TABLE scannon.rescu_miss_cid AS (
    WITH miss_ctrid AS (
        SELECT 
            da.detector_id,
            da.arterycode,
            ca.centreline_id          
        FROM scannon.rescu_det_art AS da
        LEFT JOIN scannon.cent_art AS ca USING (arterycode)
        WHERE da.centreline_id IS NULL
    )

    SELECT 
        mc.* 
    FROM miss_ctrid AS mc

    UNION

    SELECT 
        da.detector_id,
        da.arterycode,
        da.centreline_id
    FROM scannon.rescu_det_art AS da
    WHERE centreline_id IS NOT NULL
);