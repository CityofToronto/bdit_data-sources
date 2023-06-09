-- make a table that judges days of rescu data based on how many 15 minute bins there are per day 

CREATE TABLE scannon.rescu_data_days AS (

    -- count how many bins there are per day
    WITH bin_ct AS (
        SELECT
            v.arterycode,
            v.detector_id,
            date_trunc('day', v.datetime_bin)::date AS dt,
            COUNT(v.datetime_bin) AS bin_ct
        FROM rescu.volumes_15min AS v
        WHERE 
            v.datetime_bin >= '2020-01-01'
            AND v.datetime_bin < '2023-04-01'
        GROUP BY 
            v.arterycode,
            v.detector_id,
            date_trunc('day', v.datetime_bin)::date
        HAVING COUNT(v.datetime_bin) = 96 -- 96 is a perfect day of data; coming soon: look at time gaps because we can fill some and have more data days
    )

    -- count the good data days compared to total data days to get an idea of how good detectors are
    SELECT
        bc.arterycode,
        bc.detector_id,
        di.det_group,
        di.number_of_lanes,
        di.primary_road || ' and ' || di.cross_road AS gen_loc,
        ST_SETSRID(ST_MakePoint(di.longitude, di.latitude), 4326) AS geom,
        COUNT(bc.dt) AS good_data_days,
        '2023-04-01'::date - '2020-01-01'::date AS total_days
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