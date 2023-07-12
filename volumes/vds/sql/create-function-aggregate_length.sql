SELECT
    rv.division_id,
    rv.vds_id,
    datetime_bin(rv.dt, 15) AS datetime_15min,
    FLOOR(rv.length_meter) length_meter,
    COUNT(*) AS count,
    SUM(COUNT(*)) OVER (PARTITION BY rv.division_id, rv.vds_id, datetime_bin(rv.dt, 15)) AS total_count
FROM vds.raw_vdsvehicledata AS rv
WHERE 
    dt >= '2023-07-05 00:00:00'::timestamp --_start_date 
    AND dt < '2023-07-06 00:00:00'::timestamp --_end_date
GROUP BY 
    rv.division_id,
    rv.vds_id,
    datetime_bin(rv.dt, 15),
    FLOOR(rv.length_meter)