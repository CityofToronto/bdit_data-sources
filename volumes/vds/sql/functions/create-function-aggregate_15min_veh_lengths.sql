--Aggregate `vds.raw_vdsvehicledata` into table `vds.veh_length_15min` by detector / 15min bins / 1m length bins.
INSERT INTO vds.veh_length_15min (division_id, vds_id, datetime_15min, length_meter, count,
    total_count)

SELECT
    division_id,
    vds_id,
    datetime_bin(dt, 15) AS datetime_15min,
    FLOOR(length_meter) AS length_meter,
    COUNT(*) AS count,
    SUM(COUNT(*)) OVER (
        PARTITION BY division_id, vds_id, datetime_bin(dt, 15)
    ) AS total_count
FROM vds.raw_vdsvehicledata
WHERE
    dt >= '{{ ds }} 00:00:00'::timestamp
    AND dt < '{{ ds }} 00:00:00'::timestamp + INTERVAL '1 DAY'
    AND length_meter IS NOT NULL
GROUP BY 
    division_id,
    vds_id,
    datetime_bin(dt, 15),
    FLOOR(length_meter)
ON CONFLICT DO NOTHING;