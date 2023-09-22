-- make a table of rescu gaps in 2021
CREATE TABLE scannon.rescu_gaps_21 AS (
    
    -- find the time difference between all bins tart times (should be 15 minutes) in 2021
    WITH bin_time AS (
        SELECT 
            v.detector_id,
            v.datetime_bin,
            CASE
                WHEN v.datetime_bin - LAG(v.datetime_bin, 1) OVER (PARTITION BY v.detector_id ORDER BY v.datetime_bin) = '00:15:00' THEN 0
                ELSE 1
            END AS bin_break,
            v.datetime_bin - LAG(v.datetime_bin, 1) OVER (PARTITION BY v.detector_id ORDER BY v.datetime_bin) AS bin_gap            
        FROM rescu.volumes_15min AS v
        WHERE  
            v.datetime_bin::date >= '2021-01-01'
            AND v.datetime_bin::date < '2022-01-01'
    )

-- calculate the start and end times of gaps that are longer than 15 minutes
    SELECT
        bt.detector_id,
        bt.datetime_bin::date AS gap_dt,
        bt.datetime_bin - bt.bin_gap AS gap_start_tx,
        bt.datetime_bin AS gap_end_tx,
        bt.bin_gap
    FROM bin_time AS bt
    WHERE 
        bt.bin_break = 1
        AND bt.bin_gap IS NOT NULL
);