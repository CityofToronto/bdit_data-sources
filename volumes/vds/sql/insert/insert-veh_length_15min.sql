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
    dt >= {{ ds }}::timestamp
    AND dt < {{ ds }}::timestamp + INTERVAL '1 DAY'
    AND rv.length_meter IS NOT NULL
GROUP BY 
    rv.division_id,
    rv.vds_id,
    datetime_bin(rv.dt, 15),
    FLOOR(rv.length_meter)
ON CONFLICT DO NOTHING;