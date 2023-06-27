CREATE OR REPLACE FUNCTION gwolofs.aggregate_15min_vds_volumes(_start_date timestamp, _end_date timestamp
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

BEGIN
	
    --Aggregated into speed bins and 1 hour bin
    INSERT INTO gwolofs.vds_volumes_15min (divisionid, vdsid, datetime_bin, volume_15min)
    SELECT 
        divisionid,
        vdsid,
        datetime_15min,
        SUM(volumeVehiclesPerHour) / 4 / 45 AS volume_15min
    FROM gwolofs.raw_vdsdata
    --add join to detector inventory later
    WHERE 
        datetime_15min >= _start_date 
        AND datetime_15min < _end_date
    GROUP BY
        divisionid,
        vdsid,
        datetime_15min
    ON CONFLICT DO NOTHING;

END;

$BODY$;

--GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour() to wys_bot;