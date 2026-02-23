-- make a table that contains stats by lane so that we can set volume minimums
CREATE TABLE scannon.rescu_lane_stats_21 AS (
    
    -- calculate daily volumes for days when detectors have data for every 15 minute bin 
    WITH daily_vol AS (
        SELECT
            v.arterycode,
            v.detector_id,
            date_trunc('day', v.datetime_bin)::date AS dt,
            COUNT(v.datetime_bin) AS bin_ct,
            SUM(v.volume_15min) AS daily_vol
        FROM rescu.volumes_15min AS v
        WHERE 
            datetime_bin >= '2021-01-01'
            AND datetime_bin < '2022-01-01'
        GROUP BY 
            v.arterycode,
            v.detector_id,
            date_trunc('day', v.datetime_bin)::date
        HAVING COUNT(v.datetime_bin) >= 96
    ),

    -- figure out if the day is a weekend or weekday; add in some more detector info
    day_jam AS (
        SELECT 
            dv.arterycode,
            dv.detector_id,
            di.det_group,
            di.number_of_lanes,
            dv.dt,
            di.primary_road || ' and ' || di.cross_road AS gen_loc,
            TO_CHAR(dv.dt, 'Day') AS dow,
            CASE
                WHEN EXTRACT(ISODOW FROM dv.dt) IN (6, 7) THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type
        FROM daily_vol AS dv
        LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
        WHERE 
            di.latitude IS NOT NULL 
            AND di.longitude IS NOT NULL
            AND dv.dt >= '2021-01-01'
            AND dv.dt < '2022-01-01'
    ),

    -- calculate stats
    ave_vol AS (
        SELECT 
            dj.arterycode,
            dj.detector_id,
            dj.det_group,
            dj.number_of_lanes,
            dj.gen_loc,
            dj.day_type,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dv.daily_vol) AS med_vol,
            ROUND(AVG(dv.daily_vol), 0) AS a_vol,
            ROUND(STDDEV_POP(dv.daily_vol), 0) AS stdev_vol,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dv.daily_vol) / dj.number_of_lanes AS lane_med_vol,
            ROUND(AVG(dv.daily_vol) / dj.number_of_lanes, 0) AS lane_a_vol,
            ROUND(STDDEV_POP(dv.daily_vol) / dj.number_of_lanes, 0) AS lane_stdev_vol
        FROM day_jam AS dj
        LEFT JOIN daily_vol AS dv 
            ON dv.detector_id = dj.detector_id
                AND dv.dt = dj.dt
        WHERE dj.gen_loc NOT LIKE 'RAMP%'
        GROUP BY 
            dj.arterycode,
            dj.detector_id,
            dj.det_group,
            dj.number_of_lanes,
            dj.gen_loc,    
            dj.day_type            
    )

    -- final table - add in mapped location
    SELECT
        av.detector_id,
        av.det_group,
        av.day_type,
        av.lane_med_vol::int AS lane_med_vol,
        av.lane_a_vol,
        ST_SETSRID(ST_MakePoint(di.longitude, di.latitude), 4326) AS geom
    FROM ave_vol AS av
    LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
    ORDER BY av.day_type, av.det_group, av.detector_id
);