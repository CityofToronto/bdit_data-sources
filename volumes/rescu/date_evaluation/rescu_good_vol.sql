-- Make a table that stores 15 minute volume data from RESCU detectors that recorded enough volume
CREATE TABLE scannon.rescu_good_vol_20 AS (

    -- I initially made this table because I was running the query using subsets of the data for speeeeeeeed and now I see I don't need it but anyway
    WITH ev_sub AS (
        SELECT 
            ev.detector_id,
            ev.arterycode,
            ev.gen_loc,
            ev.dt
        FROM scannon.rescu_enuf_vol_20 AS ev
    )
    
    -- Using geo_id as a proxy for centreline_id 
    SELECT 
        es.detector_id,
        es.arterycode,
        art.geo_id,
        es.gen_loc,
        v.datetime_bin,
        v.volume_15min
    FROM ev_sub AS es
    LEFT JOIN traffic.arterydata AS art USING(arterycode)
    LEFT JOIN rescu.volumes_15min AS v 
        ON es.detector_id = v.detector_id
        AND es.dt = v.datetime_bin::date
);