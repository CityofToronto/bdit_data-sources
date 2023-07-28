SELECT
    d.dt::date AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
    COUNT(*) AS count
FROM vds.raw_vdsvehicledata AS d
WHERE
    d.division_id = 2 --8001 and 8046 have only null values for speed/length/occupancy
    AND dt >= {start}::timestamp - interval {lookback}
    AND dt < {start}::timestamp
GROUP BY dt