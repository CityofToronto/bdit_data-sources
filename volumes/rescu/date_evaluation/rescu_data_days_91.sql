-- Make a table of the number of good data days for each RESCU detector where a good data day has 91 or more 15 minute bins (96 is the max) 
CREATE TABLE scannon.rescu_data_days_91 AS (

    --first, count the bins and only move the detectors with 91 or more bins per day
    WITH bin_ct AS (
        SELECT
            v.arterycode,
            v.detector_id,
            date_trunc('day', v.datetime_bin)::date AS dt,
            COUNT(v.datetime_bin) AS bin_ct
        FROM rescu.volumes_15min AS v
        WHERE 
            v.datetime_bin >= '2021-01-01'
            AND v.datetime_bin < '2022-01-01'
        GROUP BY 
            v.arterycode,
            v.detector_id,
            date_trunc('day', v.datetime_bin)::date
        HAVING COUNT(v.datetime_bin) >= 91
)

    -- here's a table of those detectors with helpful attribute info including location
    SELECT
        bc.arterycode,
        bc.detector_id,
        di.det_group,
        di.number_of_lanes,
        di.primary_road || ' and ' || di.cross_road AS gen_loc,
        ST_SETSRID(ST_MakePoint(di.longitude, di.latitude), 4326) AS geom,
        COUNT(bc.dt) AS good_data_days,
        '2022-01-01'::date - '2021-01-01'::date AS total_days
    FROM bin_ct AS bc
    LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
    WHERE ST_SETSRID(ST_MakePoint(di.longitude, di.latitude), 4326) IS NOT NULL
    GROUP BY
        bc.arterycode,
        bc.detector_id,
        di.det_group,
        di.number_of_lanes,
        di.primary_road || ' and ' || di.cross_road,
        ST_SETSRID(ST_MakePoint(di.longitude, di.latitude), 4326)
    ORDER BY
        COUNT(bc.dt) DESC
);