CREATE OR REPLACE FUNCTION vds.aggregate_15min_vds_volumes(_start_date timestamp, _end_date timestamp)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

BEGIN
	
    --Aggregated into speed bins and 1 hour bin
    INSERT INTO vds.volumes_15min (division_id, vds_id, detector_id, datetime_bin, volume_15min)
    
    SELECT 
        d.division_id,
        d.vds_id,
        c.detector_id,
        d.datetime_15min,
        SUM(d.volume_veh_per_hr) / 4 / 45 AS volume_15min 
            -- / 4 to convert hourly volume to 15 minute volume
            -- / 45 to get average of 45 x 20 sec bins per 15 minutes (assumes blanks are 0)
    FROM vds.raw_vdsdata AS d
    JOIN vds.vdsconfig AS c ON
        d.vds_id = c.vds_id
        AND d.division_id = c.division_id
        AND d.datetime_15min >= c.start_timestamp
        AND (
            d.datetime_15min <= c.end_timestamp
            OR c.end_timestamp IS NULL) --no end date
    WHERE 
        datetime_15min >= _start_date --'2023-06-01 01:00:00'::timestamp 
        AND datetime_15min < _end_date --'2023-06-01 02:00:00'::timestamp
    GROUP BY
        d.division_id,
        d.vds_id,
        c.detector_id,
        d.datetime_15min
    ON CONFLICT DO NOTHING;

END;

$BODY$;

GRANT EXECUTE ON FUNCTION vds.aggregate_15min_vds_volumes(timestamp, timestamp) to vds_bot;