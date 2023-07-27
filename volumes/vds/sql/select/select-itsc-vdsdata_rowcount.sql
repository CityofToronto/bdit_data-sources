SELECT
    TIMEZONE('EST5EDT', TO_TIMESTAMP(timestamputc))::date AS dt, 
    SUM(LENGTH(lanedata)/15) AS count --each 15 bytes represents one row in final expanded data
FROM public.vdsdata
WHERE
    divisionid = 2 --other is 8001 which are traffic signal detectors and are mostly empty
    AND timestamputc >= extract(EPOCH FROM {start}::timestamptz - INTERVAL {lookback})::integer
    AND timestamputc < extract(EPOCH FROM {start}::timestamptz)::integer
    AND length(lanedata) > 0
GROUP BY dt;