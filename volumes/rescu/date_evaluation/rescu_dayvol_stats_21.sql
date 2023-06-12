-- Make a table that compares daily volumes with the average daily volumes and z scores
CREATE TABLE scannon.rescu_dayvol_stats_21 AS (

-- determine what day it is (weekday or weekend)    
    WITH day_jam AS (
        SELECT 
            v.arterycode,
            v.detector_id,
            v.datetime_bin,
            CASE
                WHEN EXTRACT(ISODOW FROM v.datetime_bin) IN (6, 7) THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type,
            di.primary_road || ' and ' || di.cross_road AS gen_loc,
            date_trunc('day', v.datetime_bin)::date AS dt,
            TO_CHAR(v.datetime_bin, 'Day') AS dow
        FROM rescu.volumes_15min AS v
        LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
        WHERE 
            di.latitude IS NOT NULL 
            AND di.longitude IS NOT NULL
            AND v.datetime_bin >= '2021-01-01'
            AND v.datetime_bin < '2022-01-01'
    ),

    -- calculate daily volumes
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
                AND dj.datetime_bin = v.datetime_bin
        WHERE 
            v.datetime_bin >= '2021-01-01'
            AND v.datetime_bin < '2022-01-01'
        GROUP BY
            dj.arterycode,
            dj.detector_id,
            dj.gen_loc,
            dj.dt,
            dj.day_type
    ),

    -- calculate average and st dev volumes for weekdays and weekends
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

    -- put the average and st dev volumes with the daily volumes
    SELECT
        av.arterycode,
        av.detector_id,
        av.gen_loc,
        --rdd.good_data_days,
        --rdd.geom,
        dv.dt,
        av.day_type,
        dv.d_vol,
        av.a_vol,
        av.stdev_vol,
        ROUND(((dv.d_vol - av.a_vol) / av.stdev_vol), 0) AS zscore
    FROM daily_vol AS dv
    LEFT JOIN ave_vol AS av 
        ON av.detector_id = dv.detector_id 
            AND av.day_type = dv.day_type
    WHERE av.stdev_vol > 0
);