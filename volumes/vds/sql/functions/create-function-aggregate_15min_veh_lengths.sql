CREATE OR REPLACE FUNCTION vds.aggregate_15min_veh_lengths(_start_date timestamp, _end_date timestamp)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

BEGIN
	
    --Aggregated into speed bins and 1 hour bin
    INSERT INTO vds.veh_length_15min (division_id, vds_id, datetime_15min, length_meter, count, total_count)

    SELECT
        rv.division_id,
        rv.vds_id,
        datetime_bin(rv.dt, 15) AS datetime_15min,
        FLOOR(rv.length_meter) length_meter,
        COUNT(*) AS count,
        SUM(COUNT(*)) OVER (PARTITION BY rv.division_id, rv.vds_id, datetime_bin(rv.dt, 15)) AS total_count
    FROM vds.raw_vdsvehicledata AS rv
    WHERE 
        dt >= _start_date 
        AND dt < _end_date
        AND rv.length_meter IS NOT NULL
    GROUP BY 
        rv.division_id,
        rv.vds_id,
        datetime_bin(rv.dt, 15),
        FLOOR(rv.length_meter)
    ON CONFLICT DO NOTHING;

END;

$BODY$;

GRANT EXECUTE ON FUNCTION vds.aggregate_15min_vds_lengths(timestamp, timestamp) to vds_bot;

COMMENT ON FUNCTION vds.aggregate_15min_vds_lengths IS 'Function to aggregate `vds.raw_vdsvehicledata` into table `vds.veh_length_15min` by detector / 15min bins / 1m length bins.';
