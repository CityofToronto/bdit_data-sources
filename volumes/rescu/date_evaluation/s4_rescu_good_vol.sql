-- Make a table that stores 15 minute volume data from RESCU detectors that recorded enough volume
DROP TABLE scannon.rescu_good_vol_21;
CREATE TABLE scannon.rescu_good_vol_21 AS (

    -- Using THE TABLE of centreline_id and arterycode as directed by MOVE; it lives on the flashcrow server as counts.arteries_centreline 
    SELECT 
        ev.detector_id,
        ev.arterycode,
        art.centreline_id,
        ev.gen_loc,
        di.direction,
        v.datetime_bin,
        v.volume_15min
    FROM scannon.rescu_enuf_vol_21 AS ev
    LEFT JOIN scannon.cent_art AS art USING (arterycode)
    LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
    LEFT JOIN rescu.volumes_15min AS v 
        ON ev.detector_id = v.detector_id
            AND ev.dt = v.datetime_bin::date
);