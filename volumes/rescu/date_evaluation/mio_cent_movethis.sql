CREATE TABLE scannon.mio_cent AS (
    SELECT 
        mq.*,
        cm.centreline_id,
        cm.leg
    FROM scannon.miovision_qc AS mq
    LEFT JOIN scannon.centerline_miovision AS cm USING (intersection_uid)
);