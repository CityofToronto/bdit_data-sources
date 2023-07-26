CREATE OR REPLACE FUNCTION vds.aggregate_15min_veh_speeds(
    _start_date timestamp, _end_date timestamp
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE SECURITY DEFINER
AS $BODY$

BEGIN
	
    --Aggregated into 5kph speed and 15 minute bins
    INSERT INTO vds.veh_speeds_15min (division_id, vds_id, datetime_15min, speed_5kph, count, total_count)

    SELECT
        rv.division_id,
        rv.vds_id,
        datetime_bin(rv.dt, 15) AS datetime_15min,
        FLOOR(rv.speed_kmh / 5.0) * 5 speed_5kph,
        COUNT(*) AS count,
        SUM(COUNT(*)) OVER (PARTITION BY rv.division_id, rv.vds_id, datetime_bin(rv.dt, 15)) AS total_count
    FROM vds.raw_vdsvehicledata AS rv
    WHERE
        dt >= _start_date --'2023-07-05 00:00:00'::timestamp
        AND dt < _end_date --'2023-07-06 00:00:00'::timestamp
        AND rv.speed_kmh IS NOT NULL
    GROUP BY 
        rv.division_id,
        rv.vds_id,
        datetime_bin(rv.dt, 15),
        FLOOR(rv.speed_kmh / 5.0) * 5
    ON CONFLICT DO NOTHING;

END;

$BODY$;

GRANT EXECUTE ON FUNCTION vds.aggregate_15min_veh_speeds(timestamp, timestamp) TO vds_bot;

COMMENT ON FUNCTION vds.aggregate_15min_veh_speeds IS 'Function to aggregate `vds.raw_vdsvehicledata` into table `vds.veh_speeds_15min` by detector / 15min bins / 5kph speed bins.'
