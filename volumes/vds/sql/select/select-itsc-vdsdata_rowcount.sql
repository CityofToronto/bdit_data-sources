SELECT
    TIMEZONE('EST5EDT', TO_TIMESTAMP(timestamputc))::date AS dt, 
    SUM(LENGTH(lanedata)/15) AS count --each 15 bytes represents one row in final expanded data
FROM public.vdsdata
WHERE
    divisionid IN (2, 8001)
    AND timestamputc >= extract('epoch' FROM {start}::timestamptz - interval {lookback})::integer
    AND timestamputc < extract('epoch' FROM {start}::timestamptz)::integer
    AND length(lanedata) > 0
GROUP BY dt;