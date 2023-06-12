-- Make a table to calculate average daily weekday and weekend volumes

CREATE TABLE scannon.rescu_dayvol_stats_a AS (

    -- id weekend and weekdays
    WITH day_jam AS (
        SELECT 
            v.arterycode,
            v.detector_id,
            di.primary_road || ' and ' || di.cross_road AS gen_loc,
            CASE
                WHEN TO_CHAR(v.datetime_bin, 'Day') IN ('Saturday', 'Sunday') THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type,
            date_trunc('day', v.datetime_bin)::date AS dt,
            di.primary_road || ' and ' || di.cross_road AS gen_loc,
        FROM rescu.volumes_15min AS v
        LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
        WHERE 
            di.latitude IS NOT NULL 
            AND di.longitude IS NOT NULL
    ),

    -- calculate daily volume counts
    daily_vol AS (
        SELECT 
            dj.arterycode,
            dj.detector_id,
            dj.gen_loc,
            dj.dt,
            dj.day_type,
            SUM(v.volume_15min) AS d_vol
        FROM day_jam AS dj
        LEFT JOIN rescu.volumes_15min AS v 
            ON dj.detector_id = v.detector_id
            AND dj.dt = v.datetime_bin::date
        WHERE 
            v.datetime_bin >= '2022-01-01'
                AND v.datetime_bin < '2023-01-01'
        GROUP BY
            dj.arterycode,
            dj.detector_id,
            dj.gen_loc,
            dj.dt,
            dj.day_type
    ),

    -- calculate average and standard deviation of weekday and weekend volume counts
    ave_vol AS (
        SELECT 
            dv.arterycode,
            dv.detector_id,
            dv.gen_loc,
            dv.day_type,
            ROUND(AVG(dv.d_vol), 0) AS a_vol,
            ROUND(STDDEV_POP(dv.d_vol), 0) AS stdev_vol
        FROM daily_vol AS dv
        GROUP BY 
            dv.arterycode,
            dv.detector_id,
            dv.gen_loc,    
            dv.day_type            
    )

    -- calculate z scores for easy outlier identification
    SELECT
        av.arterycode,
        av.detector_id,
        av.gen_loc,
        --rdd.good_data_days,
        --rdd.geom,
        av.day_type,
        dv.d_vol,
        av.a_vol,
        av.stdev_vol,
        ROUND(((dv.d_vol - av.a_vol) / av.stdev_vol), 0) AS zscore
    FROM daily_vol AS dv
    LEFT JOIN ave_vol AS av USING (detector_id)
    --LEFT JOIN scannon.rescu_data_days AS rdd USING(detector_id)
    WHERE av.stdev_vol > 0
);