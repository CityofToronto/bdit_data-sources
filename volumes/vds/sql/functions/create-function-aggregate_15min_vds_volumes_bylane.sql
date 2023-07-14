SELECT 
    d.division_id,
    d.vds_id,
    c.detector_id,
    d.lane,
    d.datetime_15min,
    CASE WHEN
        c.detector_id LIKE 'D%' AND d.division_id = 2
            THEN SUM(d.volume_veh_per_hr) / 4 / 45 --20 sec bins
        WHEN detector_id LIKE ANY ('{"YONGE HEATH%", "YONGE DAVISVILLE%", "BCT%"}')
            THEN SUM(d.volume_veh_per_hr) / 4 / 1 --15 min bins
        WHEN detector_id LIKE ANY ('{"YONGE & DAVENPORT SMARTMICRO%", "%YONGE AND ROXBOROUGH%"}')
            THEN SUM(d.volume_veh_per_hr) / 4 / 3 --5 min bins
    END AS volume_15min,
        -- / 4 to convert hourly volume to 15 minute volume
        -- / (45, 1, 3) to get average 15 minute volume depending on bin size (assumes blanks are 0)
    CASE WHEN
        c.detector_id LIKE 'D%' AND d.division_id = 2 
            THEN 45 --20 sec bins
        WHEN detector_id LIKE ANY ('{"YONGE HEATH%", "YONGE DAVISVILLE%", "BCT%"}')
            THEN 1 --15 min bins
        WHEN detector_id LIKE ANY ('{"YONGE & DAVENPORT SMARTMICRO%", "%YONGE AND ROXBOROUGH%"}')
            THEN 3 --5 min bins
    END AS expected_bins,
    COUNT(*) AS num_obs
FROM vds.raw_vdsdata AS d
JOIN vds.vdsconfig AS c ON
    d.vds_id = c.vds_id
    AND d.division_id = c.division_id
    AND d.datetime_15min >= c.start_timestamp
    AND (
        d.datetime_15min <= c.end_timestamp
        OR c.end_timestamp IS NULL) --no end date
WHERE 
    datetime_15min >= '2023-07-05 00:00:00'::timestamp --_start_date 
    AND datetime_15min < '2023-07-06 00:00:00'::timestamp --_end_date
GROUP BY
    d.division_id,
    d.vds_id,
    c.detector_id,
    d.lane,
    d.datetime_15min