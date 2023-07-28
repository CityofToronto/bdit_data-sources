SELECT
    (TIMEZONE('UTC', timestamputc) AT TIME ZONE 'EST5EDT')::date AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
    COUNT(*) AS count
FROM public.vdsvehicledata
WHERE
    divisionid = 2 --8001 and 8046 have only null values for speed/length/occupancy
    AND timestamputc >= TIMEZONE('UTC', {start}::timestamptz - interval {lookback})
    AND timestamputc < TIMEZONE('UTC', {start}::timestamptz)
GROUP BY dt