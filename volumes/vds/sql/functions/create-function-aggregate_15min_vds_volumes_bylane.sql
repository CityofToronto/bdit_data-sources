CREATE OR REPLACE FUNCTION vds.aggregate_15min_vds_volumes_bylane(_start_date timestamp, _end_date timestamp)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

BEGIN
	
    --Aggregated into speed bins and 1 hour bin
    INSERT INTO vds.volumes_15min_bylane (division_id, vds_id, detector_id, lane, datetime_bin, volume_15min, expected_bins, num_obs)
    
    WITH detector_inventory AS (
        SELECT *, 
            CASE WHEN detector_id LIKE 'D%' AND division_id = 2
                THEN 45 --20 sec bins
            WHEN detector_id LIKE ANY ('{"YONGE HEATH%", "YONGE DAVISVILLE%", "BCT%"}')
                THEN 1 --15 min bins
            WHEN detector_id LIKE ANY ('{"YONGE & DAVENPORT SMARTMICRO%", "%YONGE AND ROXBOROUGH%"}')
                THEN 3 --5 min bins
            END AS expected_bins
        FROM vds.vdsconfig 
    )

    SELECT 
        d.division_id,
        d.vds_id,
        c.detector_id,
        d.lane,
        d.datetime_15min,
        SUM(d.volume_veh_per_hr) / 4 / c.expected_bins AS volume_15min,
            -- / 4 to convert hourly volume to 15 minute volume
            -- / (expected_bins) to get average 15 minute volume depending on bin size (assumes blanks are 0)
        c.expected_bins,
        COUNT(*) AS num_obs
    FROM vds.raw_vdsdata AS d
    JOIN detector_inventory AS c ON
        d.vds_id = c.vds_id
        AND d.division_id = c.division_id
        AND d.datetime_15min >= c.start_timestamp
        AND (
            d.datetime_15min <= c.end_timestamp
            OR c.end_timestamp IS NULL) --no end date
    WHERE 
        datetime_15min >= _start_date --'2023-07-05 00:00:00'::timestamp
        AND datetime_15min < _end_date --'2023-07-06 00:00:00'::timestamp
    GROUP BY
        d.division_id,
        d.vds_id,
        c.detector_id,
        c.expected_bins,
        d.lane,
        d.datetime_15min
    ON CONFLICT DO NOTHING;

END;

$BODY$;

GRANT EXECUTE ON FUNCTION vds.aggregate_15min_vds_volumes_bylane(timestamp, timestamp) to vds_bot;

COMMENT ON FUNCTION vds.aggregate_15min_vds_volumes_bylane IS 'Function to aggregate `vds.raw_vdsdata` into `vds.volumes_15min_bylane` table by detector / lane / 15min bins.'